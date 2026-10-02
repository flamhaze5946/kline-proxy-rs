use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
#[derive(Clone, Debug, thiserror::Error)]
#[error("{message}")]
pub struct ApiError {
    pub status: u16,
    pub code: i64,
    pub message: String,
    /// Original framework status when Spring cannot serialize its advice body.
    pub negotiation_fallback: Option<u16>,
}
pub type Result<T> = std::result::Result<T, ApiError>;
impl ApiError {
    pub fn upstream_null() -> Self {
        Self {
            status: 502,
            code: -1000,
            message: "body from call is null.".into(),
            negotiation_fallback: None,
        }
    }
    pub fn upstream_io(error: impl std::fmt::Display) -> Self {
        Self {
            status: 502,
            code: -1001,
            message: error.to_string(),
            negotiation_fallback: None,
        }
    }
    /// Spring's global advice handles binding and uncaught exceptions as 500.
    /// Explicit ApiException business errors continue to use `bad` below.
    pub fn binding(message: impl Into<String>) -> Self {
        Self {
            status: 500,
            code: -1000,
            message: message.into(),
            negotiation_fallback: Some(400),
        }
    }
    pub fn bad(code: i64, message: impl Into<String>) -> Self {
        Self {
            status: 400,
            code,
            message: message.into(),
            negotiation_fallback: None,
        }
    }
    pub fn internal(error: impl std::fmt::Display) -> Self {
        tracing::warn!(error=%error,"market operation failed");
        Self {
            status: 500,
            code: -1000,
            message: "Upstream operation failed".into(),
            negotiation_fallback: None,
        }
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        if let Some(error) = error.downcast_ref::<Self>() {
            return error.clone();
        }
        for cause in error.chain() {
            if cause.is::<reqwest::Error>()
                || cause.is::<serde_json::Error>()
                || cause.is::<std::io::Error>()
            {
                return Self::upstream_io(cause);
            }
        }
        Self::internal(error)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            StatusCode::from_u16(self.status).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(serde_json::json!({"code":self.code,"msg":self.message})),
        )
            .into_response();
        if let Some(status) = self
            .negotiation_fallback
            .and_then(|s| StatusCode::from_u16(s).ok())
        {
            response
                .extensions_mut()
                .insert(kline_service::management::NegotiationFallback(status));
        }
        response
    }
}
