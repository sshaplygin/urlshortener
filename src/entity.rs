use std::net::IpAddr;

use axum::extract::FromRequestParts;
use axum_client_ip::ClientIp;
use axum_extra::{TypedHeader, headers::Referer, headers::UserAgent};
use chrono::DateTime;
use http::request::Parts;
use serde::{Deserialize, Serialize};

use crate::db;

/// A single redirect event, queued for the analytics pipeline.
#[derive(Debug, Serialize, Deserialize)]
pub struct VisitInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_info: Option<RequestInfo>,
    pub link_info: db::LinkInfo,
    pub created_at: DateTime<chrono::Utc>,
}

/// Client metadata attached to a redirect.
///
/// Note: `ip` and `user_agent` are personal data. Apply a retention policy
/// (TTL or IP truncation) on the `visits` table if operating under GDPR.
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<IpAddr>,
}

impl RequestInfo {
    /// True when no client metadata could be collected at all. Used only to
    /// decide whether to attach this struct to a visit, never to decide whether
    /// the visit itself is worth recording.
    pub fn is_empty(&self) -> bool {
        self.user_agent.is_none() && self.referer.is_none() && self.ip.is_none()
    }
}

impl<S> FromRequestParts<S> for RequestInfo
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let user_agent = TypedHeader::<UserAgent>::from_request_parts(parts, state)
            .await
            .map(|TypedHeader(ua)| ua.to_string())
            .ok();

        let referer = TypedHeader::<Referer>::from_request_parts(parts, state)
            .await
            .map(|TypedHeader(r)| r.to_string())
            .ok();

        // `ClientIp` is an extractor, not a request extension: it resolves the
        // address from the `ClientIpSource` extension installed as a layer in
        // `main`. Reading `extensions.get::<ClientIp>()` here would always be
        // `None` and silently drop every IP.
        let ip = match ClientIp::from_request_parts(parts, state).await {
            Ok(ClientIp(ip)) => Some(ip),
            Err(err) => {
                tracing::debug!("resolve client ip: {}", err);
                None
            }
        };

        Ok(RequestInfo {
            user_agent,
            referer,
            ip,
        })
    }
}
