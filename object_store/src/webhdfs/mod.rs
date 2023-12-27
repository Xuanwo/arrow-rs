// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! An object store implementation for WebHDFS.

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::Metakey;
use tokio::io::AsyncWrite;

use crate::path::Path;
use crate::Error;
use crate::GetResult;
use crate::ListResult;
use crate::MultipartId;
use crate::ObjectMeta;
use crate::ObjectStore;
use crate::Result;

/// Configure a connection to WebHDFS.
///
/// # Example
///
/// ```
/// # use object_store::webhdfs::WebhdfsBuilder;
/// let gcs = WebhdfsBuilder::new()
///  .with_endpoint("http://127.0.0.1:9870")
///  .build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct WebhdfsBuilder {
    /// Endpoint of webdhfs
    endpoint: Option<String>,
}

impl WebhdfsBuilder {
    /// Create a new [`WebhdfsBuilder`] with default values.
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the endpoint of webhdfs (required).
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Create a [`WebhdfsStore`] instance from the provided values,
    /// consuming `self`.
    pub fn build(mut self) -> Result<WebhdfsStore> {
        tracing_subscriber::fmt::init();

        let mut builder = opendal::services::Webhdfs::default();

        if let Some(endpoint) = self.endpoint.take() {
            builder.endpoint(&endpoint);
        }

        let op = opendal::Operator::new(builder)
            .map_err(|err| format_object_store_error(err, ""))?
            .layer(opendal::layers::LoggingLayer::default())
            .finish();

        Ok(WebhdfsStore(op))
    }
}

/// An [`ObjectStore`] implementation for generic HTTP servers
///
/// See [`crate::webhdfs`] for more information
#[derive(Debug)]
pub struct WebhdfsStore(opendal::Operator);

impl std::fmt::Display for WebhdfsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "webhdfs")
    }
}

#[async_trait]
impl ObjectStore for WebhdfsStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        Ok(self
            .0
            .write(location.as_ref(), bytes)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?)
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> crate::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(crate::Error::NotImplemented)
    }

    async fn abort_multipart(
        &self,
        _location: &Path,
        _multipart_id: &MultipartId,
    ) -> Result<()> {
        Err(super::Error::NotImplemented)
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let r = self
            .0
            .reader(location.as_ref())
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        let stream = r
            .map_err(|source| Error::Generic {
                store: "IoError",
                source: Box::new(source),
            })
            .boxed();

        Ok(GetResult::Stream(stream))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let bs = self
            .0
            .range_read(location.as_ref(), range.start as u64..range.end as u64)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(Bytes::from(bs))
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let meta = self
            .0
            .stat(location.as_ref())
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(format_object_meta(location.as_ref(), &meta)?)
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.0
            .delete(location.as_ref())
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(())
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let path = prefix.map_or("".into(), |x| format!("{x}/"));

        let stream = self
            .0
            .scan(&path)
            .await
            .map_err(|err| format_object_store_error(err, &path))?;

        let stream = stream
            .try_filter_map(|entry| async {
                // Use opendal's metadata feature to reuse the meta during list.
                let meta = self
                    .0
                    .metadata(
                        &entry,
                        Metakey::ContentLength | Metakey::LastModified | Metakey::Etag,
                    )
                    .await?;

                // If this entry is a directory, we skip it.
                if meta.is_dir() {
                    return Ok(None);
                }
                // If this entry is the prefix itself, we skip it.
                // if entry.path() == path {
                //     return Ok(None);
                // }

                Ok(Some((entry, meta)))
            })
            .then(|res| async {
                let (entry, meta) =
                    res.map_err(|err| format_object_store_error(err, ""))?;

                tracing::debug!("we got: {}", entry.path());

                Ok(format_object_meta(entry.path(), &meta)?)
            });

        Ok(stream.boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let path = prefix.map_or("".into(), |x| format!("{x}/"));

        let mut stream = self
            .0
            .list(&path)
            .await
            .map_err(|err| format_object_store_error(err, &path))?;

        let mut common_prefixes = Vec::new();
        let mut objects = Vec::new();

        while let Some(res) = stream.next().await {
            let entry = res.map_err(|err| format_object_store_error(err, &path))?;

            let meta = self
                .0
                .metadata(
                    &entry,
                    Metakey::Mode
                        | Metakey::ContentLength
                        | Metakey::LastModified
                        | Metakey::Etag,
                )
                .await
                .map_err(|err| format_object_store_error(err, entry.path()))?;

            tracing::debug!("we got: {}", entry.path());

            if meta.is_dir() {
                common_prefixes.push(entry.path().into());
            } else {
                objects.push(format_object_meta(entry.path(), &meta)?);
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.0
            .copy(from.as_ref(), to.as_ref())
            .await
            .map_err(|err| format_object_store_error(err, from.as_ref()))?;

        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if let Ok(true) = self.0.is_exist(to.as_ref()).await {
            return Err(format_object_store_error(
                opendal::Error::new(
                    opendal::ErrorKind::AlreadyExists,
                    "destination already exists",
                ),
                to.as_ref(),
            ));
        }

        self.0
            .copy(from.as_ref(), to.as_ref())
            .await
            .map_err(|err| format_object_store_error(err, from.as_ref()))?;

        Ok(())
    }
}

fn format_object_store_error(err: opendal::Error, path: &str) -> Error {
    match err.kind() {
        opendal::ErrorKind::NotFound => crate::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        },
        opendal::ErrorKind::Unsupported => crate::Error::NotSupported {
            source: Box::new(err),
        },
        opendal::ErrorKind::AlreadyExists => crate::Error::AlreadyExists {
            path: path.to_string(),
            source: Box::new(err),
        },
        _ => crate::Error::Generic {
            store: "webhdfs",
            source: Box::new(err),
        },
    }
}

fn format_object_meta(path: &str, meta: &opendal::Metadata) -> Result<ObjectMeta> {
    Ok(ObjectMeta {
        location: percent_encoding::percent_decode(path.as_bytes())
            .decode_utf8_lossy()
            .as_ref()
            .into(),
        last_modified: meta.last_modified().unwrap_or_default(),
        size: meta.content_length() as usize,
        e_tag: meta.etag().map(String::from),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{
        copy_if_not_exists, list_uses_directories_correctly, list_with_delimiter,
        put_get_delete_list_opts, rename_and_copy, stream_get,
    };
    use std::env;

    // Helper macro to skip tests if TEST_INTEGRATION and the webhdfs environment
    // variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = vec!["OBJECT_STORE_WEBHDFS_ENDPOINT"];

            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = std::env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                        but variable(s) {} need to be set",
                    unset_var_names
                )
            } else if force.is_err() {
                eprintln!(
                    "skipping Webhdfs integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                let builder = WebhdfsBuilder::new().with_endpoint(
                    env::var("OBJECT_STORE_WEBHDFS_ENDPOINT")
                        .expect("already checked OBJECT_STORE_WEBHDFS_ENDPOINT"),
                );

                builder
            }
        }};
    }

    #[tokio::test]
    async fn webhdfs_test() {
        let integration = maybe_skip_integration!().build().unwrap();
        // put_get_delete_list_opts(&integration, false).await;
        // list_uses_directories_correctly(&integration).await;
        // list_with_delimiter(&integration).await;
        // rename_and_copy(&integration).await;
        // copy_if_not_exists(&integration).await;
        // stream_get(&integration).await;
    }
}
