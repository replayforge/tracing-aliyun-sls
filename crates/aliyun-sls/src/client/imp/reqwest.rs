use crate::client::headers;
use http::HeaderMap;
use reqwest::header::{HeaderName, HeaderValue};
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct HttpClient {
    inner: reqwest::Client,
}

#[must_use = "RequestBuilder does nothing until you 'send' it"]
pub(crate) struct RequestBuilder {
    inner: reqwest::RequestBuilder,
}

pub(crate) struct Response {
    pub(crate) inner: reqwest::Response,
}

pub(crate) struct StatusCode {
    pub(crate) inner: http::StatusCode,
}

pub type Error = reqwest::Error;
type Result<T, E = Error> = std::result::Result<T, E>;

impl HttpClient {
    pub(crate) async fn new(connect_timeout: Duration, request_timeout: Duration) -> Result<Self> {
        Ok(Self {
            inner: reqwest::ClientBuilder::new()
                .user_agent(headers::USER_AGENT_VALUE)
                .https_only(true)
                .connect_timeout(connect_timeout)
                .timeout(request_timeout)
                .default_headers(HeaderMap::from_iter([
                    (
                        HeaderName::from_static(headers::CONTENT_TYPE),
                        HeaderValue::from_static(headers::DEFAULT_CONTENT_TYPE),
                    ),
                    (
                        HeaderName::from_static(headers::LOG_API_VERSION),
                        HeaderValue::from_static(headers::API_VERSION),
                    ),
                    (
                        HeaderName::from_static(headers::LOG_SIGNATURE_METHOD),
                        HeaderValue::from_static(headers::SIGNATURE_METHOD),
                    ),
                ]))
                .build()?,
        })
    }

    pub fn post(&self, url: &str) -> RequestBuilder {
        RequestBuilder {
            inner: self.inner.post(url),
        }
    }
}

impl RequestBuilder {
    pub fn header<K, V>(self, key: K, value: V) -> RequestBuilder
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<http::Error>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<http::Error>,
    {
        RequestBuilder {
            inner: self.inner.header(key, value),
        }
    }

    pub fn body(self, body: Vec<u8>) -> RequestBuilder {
        RequestBuilder {
            inner: self.inner.body(body),
        }
    }

    pub async fn send(self) -> Result<Response> {
        Ok(Response {
            inner: self.inner.send().await?,
        })
    }
}

impl StatusCode {
    pub(crate) fn is_success(&self) -> bool {
        self.inner.is_success()
    }
}

impl From<StatusCode> for u16 {
    fn from(status: StatusCode) -> u16 {
        status.inner.as_u16()
    }
}

pub(crate) fn is_retryable_error(error: &Error) -> bool {
    error.is_connect() || error.is_timeout()
}

pub(crate) fn status_code_from_error(_error: &Error) -> Option<u16> {
    None
}
