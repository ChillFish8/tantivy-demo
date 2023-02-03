#[macro_use]
extern crate tracing;

mod tantivy_storage;

use crate::tantivy_storage::TantivyIndexingStore;
use anyhow::Result;
use datacake::crdt::Key;
use datacake::eventual_consistency::EventuallyConsistentStoreExtension;
use datacake::node::{
    ConnectionConfig, Consistency, DCAwareSelector, DatacakeNode, DatacakeNodeBuilder,
};
use mimalloc::MiMalloc;
use std::net::SocketAddr;
use std::time::Duration;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tokio::time::Instant;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

static KEYSPACE: &str = "tantivy";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let doc_data = std::fs::read_to_string("movies.json")?;
    let doc_data: Vec<serde_json::Value> = serde_json::from_str(&doc_data)?;

    let addresses = [
        "127.0.0.1:8080".parse::<SocketAddr>()?,
        "127.0.0.1:8081".parse::<SocketAddr>()?,
        "127.0.0.1:8082".parse::<SocketAddr>()?,
    ];

    let [node_1, node_2, node_3] = connect_nodes(addresses).await?;

    let (_index_1, indexing_store_1) = TantivyIndexingStore::open().await?;
    let (index_2, indexing_store_2) = TantivyIndexingStore::open().await?;
    let (_index_3, indexing_store_3) = TantivyIndexingStore::open().await?;

    let node_1_store = node_1
        .add_extension(EventuallyConsistentStoreExtension::new(indexing_store_1))
        .await?;
    let _node_2_store = node_2
        .add_extension(EventuallyConsistentStoreExtension::new(indexing_store_2))
        .await?;
    let _node_3_store = node_3
        .add_extension(EventuallyConsistentStoreExtension::new(indexing_store_3))
        .await?;

    let node_1_handle = node_1_store.handle_with_keyspace(KEYSPACE);

    let start = Instant::now();

    // Write on node 1
    let mut docs = Vec::new();
    for (id, line) in doc_data.into_iter().enumerate() {
        let raw = serde_json::to_vec(&line)?;
        docs.push((id as Key, raw))
    }
    let num_docs = docs.len();
    node_1_handle.put_many(docs, Consistency::All).await?;
    info!(elapsed = ?start.elapsed(), num_doc = num_docs, "Indexing complete!");

    // Read node 2
    let schema = index_2.schema();
    let reader = index_2.reader()?;
    let searcher = reader.searcher();
    let data_field = schema.get_field("data").unwrap();

    let parser = QueryParser::new(schema, vec![data_field], index_2.tokenizers().clone());
    let query = parser.parse_query("title:hello")?;

    let results = searcher.search(&query, &TopDocs::with_limit(1))?;
    for (score, addr) in results {
        let doc = searcher.doc(addr)?;
        info!("Score: {score}, Doc: {doc:?}");
    }

    Ok(())
}

async fn connect_nodes(addrs: [SocketAddr; 3]) -> Result<[DatacakeNode; 3]> {
    let connection_cfg_1 = ConnectionConfig::new(
        addrs[0],
        addrs[0],
        vec![addrs[1].to_string(), addrs[2].to_string()],
    );
    let connection_cfg_2 = ConnectionConfig::new(
        addrs[1],
        addrs[1],
        vec![addrs[0].to_string(), addrs[2].to_string()],
    );
    let connection_cfg_3 = ConnectionConfig::new(
        addrs[2],
        addrs[2],
        vec![addrs[1].to_string(), addrs[0].to_string()],
    );

    let node_1 = DatacakeNodeBuilder::<DCAwareSelector>::new(1, connection_cfg_1)
        .connect()
        .await?;

    let node_2 = DatacakeNodeBuilder::<DCAwareSelector>::new(2, connection_cfg_2)
        .connect()
        .await?;

    let node_3 = DatacakeNodeBuilder::<DCAwareSelector>::new(3, connection_cfg_3)
        .connect()
        .await?;

    node_1
        .wait_for_nodes([2, 3], Duration::from_secs(30))
        .await?;
    node_2
        .wait_for_nodes([1, 3], Duration::from_secs(30))
        .await?;
    node_3
        .wait_for_nodes([2, 1], Duration::from_secs(30))
        .await?;

    Ok([node_1, node_2, node_3])
}
