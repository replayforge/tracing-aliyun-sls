//! Aliyun SLS client

pub use self::builder::{SlsClientBuilder, SlsClientBuilderError};
use crate::{
    Log, LogGroupMetadata,
    proto::{calc_log_group_encoded_len, encode_log_group},
};
use async_lock::OnceCell;
use std::{io, sync::Arc};
use tracing::{Instrument, Level};

mod builder;
mod headers;
mod imp;
mod signer;

const INTERNAL_DIAGNOSTICS_TARGET: &str = "tracing_aliyun_sls_internal";

/// A client for sending logs to Aliyun SLS (Simple Log Service).
#[derive(Clone)]
pub struct SlsClient {
    inner: Arc<SlsClientInner>,
}

struct SlsClientInner {
    url: String,
    signer: signer::Signer,
    http_client: OnceCell<imp::HttpClient>,
    connect_timeout: std::time::Duration,
    request_timeout: std::time::Duration,
    enable_trace: bool,
    print_internal_error: bool,
    #[cfg(feature = "deflate")]
    compression_level: u8,
}

/// Error type for SLS client operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SlsClientError {
    /// Non-successful HTTP response from the SLS service.
    #[error("http error [{status}] {message}")]
    Http {
        /// HTTP status code.
        status: u16,
        /// Error message from the response.
        message: Box<str>,
    },
    /// Other HTTP client error.
    #[error("other http client error: {0}")]
    Imp(#[from] imp::Error),
    /// Failed to encode the log group.
    #[error("failed to encode log group: {0}")]
    Encode(#[source] io::Error),
}

impl SlsClientError {
    /// Return whether retrying the operation may succeed.
    ///
    /// Transport connection and timeout failures are retryable, as are HTTP
    /// statuses 408, 429, 500, 502, 503, and 504.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Http { status, .. } => is_retryable_status(*status),
            Self::Imp(error) => imp::is_retryable_error(error),
            Self::Encode(_) => false,
        }
    }
}

const fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn validate_response_status(status: u16, message: Box<str>) -> Result<(), SlsClientError> {
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(SlsClientError::Http { status, message })
    }
}

impl SlsClient {
    /// Create a new SLS client builder.
    pub fn builder() -> SlsClientBuilder<'static> {
        SlsClientBuilder::default()
    }

    /// Put a log group to Aliyun SLS.
    pub async fn put_log(&self, metadata: &LogGroupMetadata, logs: &[Log]) {
        self.try_put_log(metadata, logs).await.ok();
    }

    /// Try to put a log group to Aliyun SLS.
    pub async fn try_put_log(
        &self,
        metadata: &LogGroupMetadata,
        logs: &[Log],
    ) -> Result<(), SlsClientError> {
        let fut = async move {
            match self.put_log_inner(metadata, logs).await {
                Err(e) => {
                    if self.inner.enable_trace {
                        match &e {
                            SlsClientError::Http { status, .. } => tracing::error!(
                                target: INTERNAL_DIAGNOSTICS_TARGET,
                                status,
                                retryable = e.is_retryable(),
                                "failed to put log"
                            ),
                            _ => tracing::error!(
                                target: INTERNAL_DIAGNOSTICS_TARGET,
                                retryable = e.is_retryable(),
                                "failed to put log"
                            ),
                        }
                    } else if self.inner.print_internal_error {
                        if let SlsClientError::Http { status, .. } = &e {
                            eprintln!(
                                "[{INTERNAL_DIAGNOSTICS_TARGET}] failed to put log: HTTP {status}"
                            );
                        } else {
                            eprintln!("[{INTERNAL_DIAGNOSTICS_TARGET}] failed to put log");
                        }
                    }
                    Err(e)
                }
                Ok(()) => Ok(()),
            }
        };
        if self.inner.enable_trace {
            fut.instrument(tracing::span!(
                target: INTERNAL_DIAGNOSTICS_TARGET,
                Level::TRACE,
                "put_log"
            ))
            .await
        } else {
            fut.await
        }
    }

    async fn put_log_inner(
        &self,
        metadata: &LogGroupMetadata,
        logs: &[Log],
    ) -> Result<(), SlsClientError> {
        let http_client = self
            .inner
            .http_client
            .get_or_try_init(|| {
                imp::HttpClient::new(self.inner.connect_timeout, self.inner.request_timeout)
            })
            .await?;

        let raw_length = calc_log_group_encoded_len(metadata, logs);
        let mut buf = Vec::with_capacity(raw_length);
        encode_log_group(&mut buf, metadata, logs).map_err(SlsClientError::Encode)?;
        #[cfg(feature = "lz4")]
        let buf = lz4_flex::compress(&buf);
        #[cfg(feature = "deflate")]
        let buf = miniz_oxide::deflate::compress_to_vec_zlib(&buf, self.inner.compression_level);

        let signature = self.inner.signer.sign(raw_length, &buf);
        let builder = http_client
            .post(&self.inner.url)
            .header(headers::AUTHORIZATION, signature.authorization)
            .header(headers::CONTENT_LENGTH, buf.len().to_string())
            .header(headers::CONTENT_MD5, signature.content_md5)
            .header(headers::DATE, signature.date)
            .header(headers::LOG_BODY_RAW_SIZE, signature.raw_length);

        #[cfg(feature = "lz4")]
        let builder = builder.header(headers::LOG_COMPRESS_TYPE, "lz4");
        #[cfg(feature = "deflate")]
        let builder = builder.header(headers::LOG_COMPRESS_TYPE, "deflate");

        let res = match builder.body(buf).send().await {
            Ok(response) => response,
            Err(error) => {
                if let Some(status) = imp::status_code_from_error(&error) {
                    return Err(SlsClientError::Http {
                        status,
                        message: "non-successful response".into(),
                    });
                }
                return Err(error.into());
            }
        };
        let status = res.status();
        let is_success = status.is_success();
        let status_code = status.into();

        if self.inner.enable_trace {
            tracing::trace!(
                target: INTERNAL_DIAGNOSTICS_TARGET,
                status = status_code,
                "received SLS response"
            );
        }

        if !is_success {
            let message = res
                .text()
                .await
                .unwrap_or_else(|_| "non-successful response (body unavailable)".to_owned());
            return validate_response_status(status_code, message.into_boxed_str());
        }

        validate_response_status(status_code, Box::default())
    }
}

#[cfg(test)]
mod tests {
    use super::{SlsClientBuilder, SlsClientError, validate_response_status};

    fn http_error(status: u16) -> SlsClientError {
        SlsClientError::Http {
            status,
            message: "test".into(),
        }
    }

    #[test]
    fn retryable_statuses_are_classified() {
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(http_error(status).is_retryable(), "status {status}");
        }
    }

    #[test]
    fn permanent_statuses_are_not_retryable() {
        for status in [400, 401, 403, 404] {
            assert!(!http_error(status).is_retryable(), "status {status}");
        }
    }

    #[test]
    fn successful_status_is_accepted() {
        assert!(validate_response_status(200, Box::default()).is_ok());
    }

    #[test]
    fn trace_disabled_regression_statuses_are_errors() {
        // Status validation is deliberately independent from `enable_trace`;
        // these are the statuses that were previously ignored when tracing was off.
        for status in [500, 403, 429] {
            assert!(
                matches!(
                    validate_response_status(status, "test response".into()),
                    Err(SlsClientError::Http { status: actual, .. }) if actual == status
                ),
                "status {status}"
            );
        }
    }

    #[ignore = "requires live Aliyun credentials and network access"]
    #[tokio::test]
    async fn live_put_log() {
        use crate::proto::*;

        let builder = SlsClientBuilder::default()
            .access_key(option_env!("ACCESS_KEY").unwrap_or_default())
            .access_secret(option_env!("ACCESS_SECRET").unwrap_or_default())
            .unwrap()
            .endpoint("cn-guangzhou.log.aliyuncs.com")
            .project("playground")
            .logstore("test")
            .enable_trace(true);

        #[cfg(feature = "deflate")]
        let builder = builder.compression_level(10);

        let client = builder.build().unwrap();

        let metadata =
            LogGroupMetadata::default().with_tag(MayStaticKey::from_static("static"), "test");
        let logs = vec![Log::default().with(MayStaticKey::from_static("message"), "hello world")];

        client.put_log(&metadata, &logs).await;
    }
}
