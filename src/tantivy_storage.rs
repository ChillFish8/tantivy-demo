use datacake::crdt::{HLCTimestamp, Key};
use datacake::eventual_consistency::{BulkMutationError, DocumentMetadata, Storage};
use datacake::rpc::async_trait;
use datacake::sqlite::SqliteStorage;
use std::time::Duration;

use puppet::{derive_message, puppet_actor, ActorMailbox};
use tantivy::schema::{Field, Schema, SchemaBuilder, FAST, STORED, TEXT};
use tantivy::{Document, Index, IndexWriter, Term};
use tokio::time::interval;

pub struct TantivyIndexingStore {
    inner: SqliteStorage,
    writer: ActorMailbox<IndexerActor>,

    id_field: Field,
    data_field: Field,
}

impl TantivyIndexingStore {
    pub async fn open() -> anyhow::Result<(Index, Self)> {
        let store = SqliteStorage::open_in_memory().await?;
        let mut schema = SchemaBuilder::new();
        let id_field = schema.add_u64_field("id", FAST | STORED);
        let data_field = schema.add_json_field("data", TEXT | STORED);
        let schema = schema.build();

        let index = Index::create_in_ram(schema);
        let writer = index.writer(80_000_000)?;
        let writer = IndexerActor::spawn(id_field, writer).await;

        Ok((
            index,
            Self {
                inner: store,
                writer,
                id_field,
                data_field,
            },
        ))
    }
}

#[async_trait]
impl Storage for TantivyIndexingStore {
    type Error = StoreError;
    type DocsIter = <SqliteStorage as Storage>::DocsIter;
    type MetadataIter = <SqliteStorage as Storage>::MetadataIter;

    async fn get_keyspace_list(&self) -> Result<Vec<String>, Self::Error> {
        self.inner
            .get_keyspace_list()
            .await
            .map_err(StoreError::from)
    }

    async fn iter_metadata(&self, keyspace: &str) -> Result<Self::MetadataIter, Self::Error> {
        self.inner
            .iter_metadata(keyspace)
            .await
            .map_err(StoreError::from)
    }

    async fn remove_tombstones(
        &self,
        keyspace: &str,
        keys: impl Iterator<Item = Key> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        match self.inner.remove_tombstones(keyspace, keys).await {
            Ok(()) => Ok(()),
            Err(e) => {
                let ids = e.successful_doc_ids().to_vec();
                Err(BulkMutationError::new(
                    StoreError::Sqlite(e.into_inner()),
                    ids,
                ))
            }
        }
    }

    async fn put(
        &self,
        keyspace: &str,
        document: datacake::eventual_consistency::Document,
    ) -> Result<(), Self::Error> {
        let json_data = serde_json::from_slice(document.data()).map_err(anyhow::Error::from)?;

        let mut tantivy_doc = Document::new();
        tantivy_doc.add_u64(self.id_field, document.id());
        tantivy_doc.add_json_object(self.data_field, json_data);

        self.inner
            .put(keyspace, document)
            .await
            .map_err(StoreError::from)?;

        self.writer.send(IndexDocs(vec![tantivy_doc])).await;

        Ok(())
    }

    async fn multi_put(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = datacake::eventual_consistency::Document> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let mut tantivy_docs = Vec::new();
        let mut processed_docs = Vec::new();
        for document in documents {
            let json_data = serde_json::from_slice(document.data())
                .map_err(anyhow::Error::from)
                .map_err(|e| BulkMutationError::empty_with_error(e.into()))?;

            let mut tantivy_doc = Document::new();
            tantivy_doc.add_u64(self.id_field, document.id());
            tantivy_doc.add_json_object(self.data_field, json_data);

            tantivy_docs.push(tantivy_doc);
            processed_docs.push(document);
        }

        if let Err(e) = self
            .inner
            .multi_put(keyspace, processed_docs.into_iter())
            .await
        {
            let ids = e.successful_doc_ids().to_vec();
            return Err(BulkMutationError::new(
                StoreError::Sqlite(e.into_inner()),
                ids,
            ));
        }

        self.writer.send(IndexDocs(tantivy_docs)).await;

        Ok(())
    }

    async fn mark_as_tombstone(
        &self,
        keyspace: &str,
        doc_id: Key,
        timestamp: HLCTimestamp,
    ) -> Result<(), Self::Error> {
        self.inner
            .mark_as_tombstone(keyspace, doc_id, timestamp)
            .await?;
        self.writer.send(RemoveDocuments(vec![doc_id])).await;

        Ok(())
    }

    async fn mark_many_as_tombstone(
        &self,
        keyspace: &str,
        documents: impl Iterator<Item = DocumentMetadata> + Send,
    ) -> Result<(), BulkMutationError<Self::Error>> {
        let mut doc_ids = Vec::new();
        let mut delete_docs = Vec::new();
        for document in documents {
            doc_ids.push(document.id);
            delete_docs.push(document);
        }

        if let Err(e) = self
            .inner
            .mark_many_as_tombstone(keyspace, delete_docs.into_iter())
            .await
        {
            let ids = e.successful_doc_ids().to_vec();
            return Err(BulkMutationError::new(
                StoreError::Sqlite(e.into_inner()),
                ids,
            ));
        }

        self.writer.send(RemoveDocuments(doc_ids)).await;

        Ok(())
    }

    async fn get(
        &self,
        keyspace: &str,
        doc_id: Key,
    ) -> Result<Option<datacake::eventual_consistency::Document>, Self::Error> {
        self.inner
            .get(keyspace, doc_id)
            .await
            .map_err(StoreError::from)
    }

    async fn multi_get(
        &self,
        keyspace: &str,
        doc_ids: impl Iterator<Item = Key> + Send,
    ) -> Result<Self::DocsIter, Self::Error> {
        self.inner
            .multi_get(keyspace, doc_ids)
            .await
            .map_err(StoreError::from)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub struct IndexerActor {
    key_field: Field,
    writer: IndexWriter,
}

#[puppet_actor]
impl IndexerActor {
    pub async fn spawn(key_field: Field, writer: IndexWriter) -> ActorMailbox<Self> {
        let actor = Self { key_field, writer };

        let mailbox = actor.spawn_actor().await;
        let mailbox_clone = mailbox.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));
            loop {
                interval.tick().await;

                mailbox_clone.send(Commit).await;
            }
        });

        mailbox
    }

    #[puppet]
    async fn index_docs(&mut self, msg: IndexDocs) {
        for doc in msg.0 {
            self.writer.add_document(doc).unwrap();
        }
    }

    #[puppet]
    async fn remove_docs(&mut self, msg: RemoveDocuments) {
        for key in msg.0 {
            let term = Term::from_field_u64(self.key_field, key);
            self.writer.delete_term(term);
        }
    }

    #[puppet]
    async fn commit(&mut self, _msg: Commit) {
        self.writer.commit().unwrap();
    }
}

pub struct IndexDocs(Vec<Document>);
derive_message!(IndexDocs, ());

pub struct RemoveDocuments(Vec<Key>);
derive_message!(RemoveDocuments, ());

pub struct Commit;
derive_message!(Commit, ());
