use std::net::IpAddr;

use axum::extract::FromRequestParts;
use axum_client_ip::ClientIp;
use axum_extra::{TypedHeader, headers::Referer, headers::UserAgent};
use chrono::DateTime;
use http::request::Parts;
use serde::{Deserialize, Serialize};

use crate::db;

#[derive(Debug, Serialize, Deserialize)]
pub struct VisitInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_info: Option<RequestInfo>,
    pub link_info: db::LinkInfo,
    pub created_at: DateTime<chrono::Utc>,
}

impl VisitInfo {
    pub fn is_empty(&self) -> bool {
        self.request_info.is_none()
    }
}

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
    pub fn is_empty(&self) -> bool {
        self.user_agent.is_none() && self.referer.is_none() && self.ip.is_none()
    }
}

impl<S> FromRequestParts<S> for RequestInfo
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let user_agent = TypedHeader::<UserAgent>::from_request_parts(parts, _state)
            .await
            .map(|TypedHeader(ua)| ua.to_string())
            .ok();

        let referer = TypedHeader::<Referer>::from_request_parts(parts, _state)
            .await
            .map(|TypedHeader(r)| r.to_string())
            .ok();

        let ip = parts.extensions.get::<ClientIp>().map(|ClientIp(ip)| *ip);

        Ok(RequestInfo {
            user_agent,
            referer,
            ip,
        })
    }
}
