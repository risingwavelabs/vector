use std::{sync::Arc, task::Poll};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use tokio_postgres::{
    tls::NoTlsStream, Client, Connection, Error as RisingWaveError, NoTls, Socket,
};
use tower::Service;
use vector_common::{
    config::ComponentKey,
    finalization::{EventFinalizers, EventStatus, Finalizable},
    request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata},
};
use vector_core::{config::DataType, stream::DriverResponse};

use crate::sinks::prelude::RetryLogic;

use super::RisingWaveConfig;

#[derive(Clone, Default)]
pub struct RisingWaveRetryLogic;

impl RetryLogic for RisingWaveRetryLogic {
    type Error = RisingWaveError;
    type Response = RisingWaveResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        // TODO: Need more reasonable retry logic
        !error.is_closed()
    }
}

#[derive(Debug, Clone)]
pub struct RisingWaveRequest {
    pub body: Vec<RisingWaveBody>,
    pub metadata: RisingWaveRequestMetadata,
    pub request_metadata: RequestMetadata,
}

impl Finalizable for RisingWaveRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.metadata.finalizers)
    }
}

impl MetaDescriptive for RisingWaveRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.request_metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.request_metadata
    }
}

#[derive(Debug, Clone)]
pub struct RisingWaveBody {
    pub value: Bytes,
    pub data_type: DataType,
    pub source_id: Option<Arc<ComponentKey>>,
}

#[derive(Debug, Clone)]
pub struct RisingWaveRequestMetadata {
    pub finalizers: EventFinalizers,
}

#[derive(Debug)]
pub struct RisingWaveResponse {
    event_byte_size: GroupedCountByteSize,
    byte_size: usize,
}

impl DriverResponse for RisingWaveResponse {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.event_byte_size
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.byte_size)
    }
}

#[derive(Debug, Clone)]
pub struct RisingWaveService {
    pub schema: Option<String>,
    pub table: String,
    pub client: Arc<Client>,
}

impl RisingWaveService {
    pub async fn try_new(
        config: &RisingWaveConfig,
    ) -> crate::Result<(Self, Connection<Socket, NoTlsStream>)> {
        let pg_config = config.create_pg_config();

        let (client, connection) = pg_config.connect(NoTls).await?;

        Ok((
            Self {
                schema: config.schema.clone(),
                table: config.table.clone(),
                client: Arc::new(client),
            },
            connection,
        ))
    }
}

impl Service<RisingWaveRequest> for RisingWaveService {
    type Response = RisingWaveResponse;
    type Error = RisingWaveError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RisingWaveRequest) -> Self::Future {
        let this = self.clone();

        Box::pin(async move {
            let client = this.client;

            let schema = this.schema.map_or(String::default(), |s| format!("{}.", s));
            let table = this.table;
            let relation = schema + &table;

            let stat = format!("INSERT INTO {} VALUES ($1, $2, $3);", relation);
            let stat = client.prepare(&stat).await?;

            for body in req.body {
                let value = body.value.as_ref();
                let data_type = body.data_type.to_string();
                let source_id = body.source_id.as_ref().map(|ck| ck.id());

                let _row = client
                    .execute(&stat, &[&source_id, &data_type, &value])
                    .await?;
            }
            _ = client.execute("FLUSH;", &[]).await?;

            let req_metadata = req.request_metadata;
            let byte_size = req_metadata.events_byte_size();
            let event_byte_size = req_metadata.into_events_estimated_json_encoded_byte_size();

            Ok(RisingWaveResponse {
                event_byte_size,
                byte_size,
            })
        })
    }
}
