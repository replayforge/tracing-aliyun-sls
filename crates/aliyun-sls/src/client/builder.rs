use crate::client::{SlsClient, SlsClientInner, signer};
use async_lock::OnceCell;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use std::{sync::Arc, time::Duration};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Builder error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SlsClientBuilderError {
    /// Invalid access secret length.
    #[error("invalid access secret length")]
    Hmac,
    /// Missing required field in the builder.
    #[error("missing required field: {0}")]
    Missing(&'static str),
}

/// Builder for creating an SLS client with required and optional parameters.
pub struct SlsClientBuilder<'a> {
    access_key: Option<String>,
    hmac: Option<Hmac<Sha1>>,
    endpoint: Option<&'a str>,
    project: Option<&'a str>,
    logstore: Option<&'a str>,
    shard_key: Option<&'a str>,
    connect_timeout: Duration,
    request_timeout: Duration,
    enable_trace: bool,
    print_internal_error: bool,
    #[cfg(feature = "deflate")]
    compression_level: u8,
}

type Result<T, E = SlsClientBuilderError> = std::result::Result<T, E>;

impl Default for SlsClientBuilder<'_> {
    fn default() -> Self {
        Self {
            access_key: None,
            hmac: None,
            endpoint: None,
            project: None,
            logstore: None,
            shard_key: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            enable_trace: true,
            print_internal_error: false,
            #[cfg(feature = "deflate")]
            compression_level: 6,
        }
    }
}

impl<'a> SlsClientBuilder<'a> {
    /// Set the access key for the SLS client.
    pub fn access_key(mut self, access_key: impl Into<String>) -> Self {
        self.access_key = Some(access_key.into());
        self
    }

    /// Set the access secret for the SLS client.
    pub fn access_secret(mut self, access_secret: impl AsRef<[u8]>) -> Result<Self> {
        self.hmac = Some(
            Hmac::<Sha1>::new_from_slice(access_secret.as_ref())
                .map_err(|_| SlsClientBuilderError::Hmac)?,
        );
        Ok(self)
    }

    /// Set the endpoint for the SLS client.
    pub fn endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Set the project for the SLS client.
    pub fn project(mut self, project: &'a str) -> Self {
        self.project = Some(project);
        self
    }

    /// Set the logstore for the SLS client.
    pub fn logstore(mut self, logstore: &'a str) -> Self {
        self.logstore = Some(logstore);
        self
    }

    /// Set the shard key for the SLS client.
    pub fn shard_key(mut self, shard_key: &'a str) -> Self {
        self.shard_key = Some(shard_key);
        self
    }

    /// Set the timeout for establishing an HTTP connection.
    ///
    /// The default is 3 seconds.
    ///
    /// Nyquest 0.4 does not expose a separate connect timeout, so this setting
    /// applies to the reqwest backend.
    pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    /// Set the timeout for completing an HTTP request.
    ///
    /// The default is 10 seconds.
    pub fn request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Enable or disable tracing for the SLS client.
    ///
    /// Enabled by default.
    /// If enabled, client will log via [`tracing`](https://docs.rs/tracing/latest/tracing/) crate.
    pub fn enable_trace(mut self, enable_trace: bool) -> Self {
        self.enable_trace = enable_trace;
        self
    }

    /// Enable or disable printing internal errors to stderr.
    ///
    /// Disabled by default.
    /// If enabled and tracing is not enabled, client will print errors to stderr.
    pub fn print_internal_error(mut self, print_internal_error: bool) -> Self {
        self.print_internal_error = print_internal_error;
        self
    }

    /// Set the deflate compression level for the SLS client.
    #[cfg(feature = "deflate")]
    #[cfg_attr(docsrs, doc(cfg(feature = "deflate")))]
    pub fn compression_level(mut self, level: u8) -> Self {
        self.compression_level = level.clamp(1, 10);
        self
    }

    /// Build the SLS client with the provided configuration.
    pub fn build(self) -> Result<SlsClient> {
        let access_key = self
            .access_key
            .ok_or(SlsClientBuilderError::Missing("access_key"))?;
        let hmac = self
            .hmac
            .ok_or(SlsClientBuilderError::Missing("access_secret"))?;
        let endpoint = self
            .endpoint
            .ok_or(SlsClientBuilderError::Missing("endpoint"))?;
        let project = self
            .project
            .ok_or(SlsClientBuilderError::Missing("project"))?;
        let logstore = self
            .logstore
            .ok_or(SlsClientBuilderError::Missing("logstore"))?;

        let canonicalized_resource = match self.shard_key {
            None => format!("/logstores/{logstore}/shards/lb"),
            Some(shard_key) => format!("/logstores/{logstore}/shards/route?key={shard_key}"),
        };

        let url = format!("https://{project}.{endpoint}{canonicalized_resource}");
        #[cfg(feature = "persist")]
        let idempotent_url_prefix = format!("https://{project}.{endpoint}");

        let client = SlsClientInner {
            url,
            #[cfg(feature = "persist")]
            idempotent_url_prefix,
            #[cfg(feature = "persist")]
            logstore: logstore.to_owned(),
            signer: signer::Signer {
                hmac,
                access_key,
                canonicalized_resource,
            },
            http_client: OnceCell::new(),
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            enable_trace: self.enable_trace,
            print_internal_error: self.print_internal_error,
            #[cfg(feature = "deflate")]
            compression_level: self.compression_level,
        };

        Ok(SlsClient {
            inner: Arc::new(client),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_client(builder: SlsClientBuilder<'_>) -> SlsClient {
        builder
            .access_key("key")
            .access_secret("secret")
            .expect("test secret is valid")
            .endpoint("example.com")
            .project("project")
            .logstore("logstore")
            .build()
            .expect("test configuration is complete")
    }

    #[test]
    fn timeout_defaults_are_applied() {
        let client = build_client(SlsClientBuilder::default());

        assert_eq!(client.inner.connect_timeout, Duration::from_secs(3));
        assert_eq!(client.inner.request_timeout, Duration::from_secs(10));
    }

    #[test]
    fn timeouts_are_configurable() {
        let client = build_client(
            SlsClientBuilder::default()
                .connect_timeout(Duration::from_secs(1))
                .request_timeout(Duration::from_secs(7)),
        );

        assert_eq!(client.inner.connect_timeout, Duration::from_secs(1));
        assert_eq!(client.inner.request_timeout, Duration::from_secs(7));
    }

    #[cfg(feature = "persist")]
    #[test]
    fn persistence_destination_excludes_credentials_and_includes_logstore() {
        let first = build_client(SlsClientBuilder::default());
        let same_destination = SlsClientBuilder::default()
            .access_key("different-key")
            .access_secret("different-secret")
            .expect("test secret")
            .endpoint("example.com")
            .project("project")
            .logstore("logstore")
            .build()
            .expect("build same destination");
        let other_logstore = SlsClientBuilder::default()
            .access_key("key")
            .access_secret("secret")
            .expect("test secret")
            .endpoint("example.com")
            .project("project")
            .logstore("other")
            .build()
            .expect("build other destination");

        assert_eq!(
            first.persistence_destination_fingerprint(),
            same_destination.persistence_destination_fingerprint()
        );
        assert_ne!(
            first.persistence_destination_fingerprint(),
            other_logstore.persistence_destination_fingerprint()
        );
    }
}
