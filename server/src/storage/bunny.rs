//! Bunny Storage remote files.

use std::io::Cursor;

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::{Download, RemoteFile, StorageBackend};
use crate::error::{ErrorKind, ServerError, ServerResult};

/// The Bunny Storage remote file storage backend.
#[derive(Debug)]
pub struct BunnyBackend {
    client: Client,
    config: BunnyStorageConfig,
}

/// Bunny Storage remote file storage configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct BunnyStorageConfig {
    /// The name of the bucket.
    bucket: String,

    /// Storage API endpoint
    api_endpoint: String,

    /// Pull Zone connected to the storage
    cdn_endpoint: String,

    /// Bunny Storage credentials.
    access_key: String,
}

/// Reference to a file in a Bunny Storage bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BunnyRemoteFile {
    /// Key of the file.
    pub key: String,
}

impl BunnyBackend {
    pub async fn new(config: BunnyStorageConfig) -> ServerResult<Self> {
        Ok(Self {
            client: Client::new(),
            config,
        })
    }
}

#[async_trait]
impl StorageBackend for BunnyBackend {
    async fn upload_file(
        &self,
        name: String,
        stream: &mut (dyn AsyncRead + Unpin + Send),
    ) -> ServerResult<RemoteFile> {
        let mut body = vec![];
        stream
            .read_to_end(&mut body)
            .await
            .map_err(ServerError::storage_error)?;
        let url = format!("{}/{}/{name}", self.config.api_endpoint, self.config.bucket);
        self.client
            .put(url)
            .header("AccessKey", &self.config.access_key)
            .body(body)
            .send()
            .await
            .map_err(ServerError::storage_error)?
            .error_for_status()
            .map_err(ServerError::storage_error)?;
        Ok(RemoteFile::Bunny(BunnyRemoteFile { key: name }))
    }

    async fn delete_file(&self, name: String) -> ServerResult<()> {
        let url = format!("{}/{}/{name}", self.config.api_endpoint, self.config.bucket);
        let resp = self
            .client
            .delete(url)
            .header("AccessKey", &self.config.access_key)
            .send()
            .await
            .map_err(ServerError::storage_error)?
            .error_for_status()
            .map_err(ServerError::storage_error)?;

        tracing::debug!("delete_file -> {resp:#?}");

        Ok(())
    }

    async fn delete_file_db(&self, file: &RemoteFile) -> ServerResult<()> {
        let file = if let RemoteFile::Bunny(file) = file {
            file
        } else {
            return Err(ErrorKind::StorageError(anyhow::anyhow!(
                "Does not understand the remote file reference"
            ))
            .into());
        };

        self.delete_file(file.key.clone()).await
    }

    async fn download_file(&self, name: String, prefer_stream: bool) -> ServerResult<Download> {
        let url = format!("{}/{name}", self.config.cdn_endpoint);
        Ok(if prefer_stream {
            Download::AsyncRead(Box::new(Cursor::new(
                self.client
                    .get(url)
                    .send()
                    .await
                    .map_err(ServerError::storage_error)?
                    .error_for_status()
                    .map_err(ServerError::storage_error)?
                    .bytes()
                    .await
                    .map_err(ServerError::storage_error)?,
            )))
        } else {
            Download::Url(url)
        })
    }

    async fn download_file_db(
        &self,
        file: &RemoteFile,
        prefer_stream: bool,
    ) -> ServerResult<Download> {
        let file = if let RemoteFile::Bunny(file) = file {
            file
        } else {
            return Err(ErrorKind::StorageError(anyhow::anyhow!(
                "Does not understand the remote file reference"
            ))
            .into());
        };

        self.download_file(file.key.clone(), prefer_stream).await
    }

    async fn make_db_reference(&self, name: String) -> ServerResult<RemoteFile> {
        Ok(RemoteFile::Bunny(BunnyRemoteFile { key: name }))
    }
}
