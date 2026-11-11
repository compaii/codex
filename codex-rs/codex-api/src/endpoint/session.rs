use crate::InferenceAllowance;
use crate::auth::SharedAuthProvider;
use crate::error::ApiError;
use crate::provider::Provider;
use crate::telemetry::run_with_request_telemetry;
use codex_client::EncodedJsonBody;
use codex_client::HttpTransport;
use codex_client::Request;
use codex_client::RequestBody;
use codex_client::RequestTelemetry;
use codex_client::Response;
use codex_client::StreamResponse;
use codex_client::TransportError;
use futures::StreamExt;
use http::HeaderMap;
use http::Method;
use serde_json::Value;
use std::sync::Arc;
use tracing::instrument;

pub(crate) struct EndpointSession<T: HttpTransport> {
    transport: T,
    provider: Provider,
    auth: SharedAuthProvider,
    request_telemetry: Option<Arc<dyn RequestTelemetry>>,
    inference_allowance: Option<Arc<InferenceAllowance>>,
}

impl<T: HttpTransport> EndpointSession<T> {
    pub(crate) fn new(transport: T, provider: Provider, auth: SharedAuthProvider) -> Self {
        Self {
            transport,
            provider,
            auth,
            request_telemetry: None,
            inference_allowance: None,
        }
    }

    pub(crate) fn with_request_telemetry(
        mut self,
        request: Option<Arc<dyn RequestTelemetry>>,
    ) -> Self {
        self.request_telemetry = request;
        self
    }

    pub(crate) fn with_inference_allowance(mut self, allowance: Arc<InferenceAllowance>) -> Self {
        self.inference_allowance = Some(allowance);
        self
    }

    async fn claim_inference_attempt(
        &self,
        request: &mut Request,
    ) -> Result<Option<crate::ProviderAttempt>, TransportError> {
        if let Some(allowance) = &self.inference_allowance {
            if !self.transport.supports_single_attempt() {
                return Err(TransportError::Build(
                    "metered inference requires a transport without hidden replays".into(),
                ));
            }
            let claimed = allowance
                .claim_authorized_attempt()
                .await
                .map_err(|e| TransportError::Build(e.to_string()))?;
            let remaining = claimed.remaining;
            request.timeout = Some(
                request
                    .timeout
                    .map_or(remaining, |timeout| timeout.min(remaining)),
            );
            return Ok(Some(claimed.attempt));
        }
        Ok(None)
    }

    async fn within_allowance_deadline<R>(
        &self,
        operation: impl std::future::Future<Output = Result<R, TransportError>>,
    ) -> Result<R, ApiError> {
        let result = if let Some(allowance) = &self.inference_allowance {
            tokio::time::timeout_at(allowance.deadline(), operation)
                .await
                .map_err(|_| ApiError::InvalidRequest {
                    message: "native inference allowance deadline expired".into(),
                })?
        } else {
            operation.await
        };
        result.map_err(|error| self.map_transport_error(error))
    }

    fn validate_request_body(&self, request: &Request) -> Result<(), TransportError> {
        if let Some(allowance) = &self.inference_allowance {
            let bytes = match request.body.as_ref() {
                Some(RequestBody::Json(value)) => serde_json::to_vec(value)
                    .map_err(|error| TransportError::Build(error.to_string()))?
                    .len(),
                Some(RequestBody::EncodedJson(value)) => value.as_bytes().len(),
                Some(RequestBody::Raw(value)) => value.len(),
                None => 0,
            };
            allowance
                .validate_request(bytes)
                .map_err(|error| TransportError::Build(error.to_string()))?;
        }
        Ok(())
    }

    async fn authorize_request(&self, request: Request) -> Result<Request, TransportError> {
        // Authentication may sign prepared bytes, but a bounded request cannot
        // replace the already-validated body with an unmetered logical payload.
        let request = if self.inference_allowance.is_some() {
            self.validate_request_body(&request)?;
            request.into_prepared().map_err(TransportError::Build)?
        } else {
            request
        };
        let expected_body = self
            .inference_allowance
            .as_ref()
            .map(|_| request.body.clone());
        let request = self
            .auth
            .apply_auth(request)
            .await
            .map_err(TransportError::from)?;
        if expected_body.is_some_and(|body| body != request.body) {
            return Err(TransportError::Build(
                "bounded inference authentication changed the validated request body".into(),
            ));
        }
        Ok(request)
    }

    fn map_transport_error(&self, error: TransportError) -> ApiError {
        match error {
            TransportError::Build(message) if self.inference_allowance.is_some() => {
                ApiError::InvalidRequest { message }
            }
            other => other.into(),
        }
    }

    pub(crate) fn inference_allowance(&self) -> Option<&Arc<InferenceAllowance>> {
        self.inference_allowance.as_ref()
    }

    pub(crate) fn provider(&self) -> &Provider {
        &self.provider
    }

    fn make_request(
        &self,
        method: &Method,
        path: &str,
        extra_headers: &HeaderMap,
        body: Option<&RequestBody>,
    ) -> Request {
        let mut req = self.provider.build_request(method.clone(), path);
        req.headers.extend(extra_headers.clone());
        if let Some(body) = body {
            req.body = Some(body.clone());
        }
        req
    }

    pub(crate) async fn execute(
        &self,
        method: Method,
        path: &str,
        extra_headers: HeaderMap,
        body: Option<Value>,
    ) -> Result<Response, ApiError> {
        self.execute_with(method, path, extra_headers, body, |_| {})
            .await
    }

    #[instrument(
        name = "endpoint_session.execute_with",
        level = "info",
        skip_all,
        fields(http.method = %method, api.path = path)
    )]
    pub(crate) async fn execute_with<C>(
        &self,
        method: Method,
        path: &str,
        extra_headers: HeaderMap,
        body: Option<Value>,
        configure: C,
    ) -> Result<Response, ApiError>
    where
        C: Fn(&mut Request),
    {
        if let Some(allowance) = &self.inference_allowance {
            let bytes = body
                .as_ref()
                .map(serde_json::to_vec)
                .transpose()
                .map_err(|error| ApiError::InvalidRequest {
                    message: error.to_string(),
                })?
                .map_or(0, |bytes| bytes.len());
            allowance
                .validate_request(bytes)
                .map_err(|error| ApiError::InvalidRequest {
                    message: error.to_string(),
                })?;
        }
        let body = body.map(RequestBody::Json);
        let make_request = || {
            let mut req = self.make_request(&method, path, &extra_headers, body.as_ref());
            configure(&mut req);
            req
        };

        let operation = run_with_request_telemetry(
            self.provider.retry.to_policy(),
            self.request_telemetry.clone(),
            make_request,
            |req| {
                let transport = &self.transport;
                async move {
                    let mut req = self.authorize_request(req).await?;
                    self.claim_inference_attempt(&mut req).await?;
                    transport.execute(req).await
                }
            },
        );
        let response = self.within_allowance_deadline(operation).await?;

        Ok(response)
    }

    #[instrument(
        name = "endpoint_session.stream_encoded_json_with",
        level = "info",
        skip_all,
        fields(http.method = %method, api.path = path)
    )]
    pub(crate) async fn stream_encoded_json_with<C>(
        &self,
        method: Method,
        path: &str,
        extra_headers: HeaderMap,
        body: Option<EncodedJsonBody>,
        configure: C,
    ) -> Result<(StreamResponse, Option<crate::ProviderAttempt>), ApiError>
    where
        C: Fn(&mut Request),
    {
        if let Some(allowance) = &self.inference_allowance {
            allowance
                .validate_request(body.as_ref().map_or(0, |body| body.as_bytes().len()))
                .map_err(|error| ApiError::InvalidRequest {
                    message: error.to_string(),
                })?;
        }
        let body = body.map(RequestBody::EncodedJson);
        let mut request = self.make_request(&method, path, &extra_headers, body.as_ref());
        configure(&mut request);
        self.validate_request_body(&request)
            .map_err(|error| self.map_transport_error(error))?;
        let request = request.into_prepared().map_err(TransportError::Build)?;
        let make_request = || request.clone();

        let operation = run_with_request_telemetry(
            self.provider.retry.to_policy(),
            self.request_telemetry.clone(),
            make_request,
            |req| {
                let transport = &self.transport;
                async move {
                    let mut req = self.authorize_request(req).await?;
                    let attempt = self.claim_inference_attempt(&mut req).await?;
                    let response = transport.stream(req).await?;
                    Ok((response, attempt))
                }
            },
        );
        let (mut stream, attempt) = self.within_allowance_deadline(operation).await?;
        if let Some(allowance) = &self.inference_allowance {
            let deadline = allowance.deadline();
            // Keep the original absolute deadline while consuming the body too;
            // a relative request timeout starts after transport preparation.
            stream.bytes = futures::stream::unfold(Some(stream.bytes), move |state| async move {
                let mut bytes = state?;
                match tokio::time::timeout_at(deadline, bytes.next()).await {
                    Ok(Some(chunk)) => Some((chunk, Some(bytes))),
                    Ok(None) => None,
                    Err(_) => Some((Err(TransportError::Timeout), None)),
                }
            })
            .boxed();
        }

        Ok((stream, attempt))
    }
}
