use std::sync::Arc;

use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::{Client, Config, Connection, Socket};

use crate::codecs::EncodingConfig;
use crate::sinks::prelude::*;

use super::service::{RisingWaveRetryLogic, RisingWaveService};
use super::sink::RisingWaveSink;

/// Configuration for the `risingwave` sink.
#[configurable_component(sink("risingwave", "Deliver log data to a RisingWave database."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct RisingWaveConfig {
    /// The hostname or IP address of RisingWave.
    #[configurable(metadata(docs::examples = "localhost"))]
    pub host: String,

    /// The port number of RisingWave.
    #[configurable(metadata(docs::examples = "5432"))]
    pub port: u16,

    /// The name of the database to connect to.
    #[configurable(metadata(docs::examples = "dev"))]
    pub database: String,

    /// The schema within the database.
    #[configurable(metadata(docs::examples = "public"))]
    pub schema: Option<String>,

    /// The table that data is inserted into.
    #[configurable(metadata(docs::examples = "mytable"))]
    pub table: String,

    /// The RisingWave user that has write access to the table.
    #[configurable(metadata(docs::examples = "myuser"))]
    pub user: String,

    /// The password for the user.
    #[configurable(metadata(docs::examples = "mypassword"))]
    pub password: Option<String>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    pub encoding: EncodingConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub(super) batch: BatchConfig<RisingWaveDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl RisingWaveConfig {
    pub fn create_pg_config(&self) -> Config {
        let mut pg_config = Config::new();
        pg_config
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user);
        // Set the password if it's provided
        if let Some(ref pw) = self.password {
            pg_config.password(pw);
        }
        pg_config
    }
}

impl GenerateConfig for RisingWaveConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"
            host = "localhost"
            port = 4566
            database = "dev"
            user = "root"
            table = "t"
            encoding.codec = "json"
            "#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "risingwave")]
impl SinkConfig for RisingWaveConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let request_settings = self.request.unwrap_with(&TowerRequestConfig::default());
        let (service, connection) = RisingWaveService::try_new(self).await?;
        let client = service.client.clone();

        let service = ServiceBuilder::new()
            .settings(request_settings, RisingWaveRetryLogic)
            .service(service);

        let transformer = self.encoding.transformer();
        let serializer = self.encoding.build()?;
        let encoder = Encoder::<()>::new(serializer);

        let sink = RisingWaveSink {
            transformer,
            encoder,
            service,
            batch_settings: self.batch.into_batcher_settings()?,
        };

        // Healthcheck could be a simple query to the Risingwave database
        let healthcheck = healthcheck(client, connection).boxed();

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        // TODO: Will support metrics in the future. https://github.com/risingwavelabs/vector/issues/1
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

pub(crate) async fn healthcheck(
    client: Arc<Client>,
    connection: Connection<Socket, NoTlsStream>,
) -> crate::Result<()> {
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(?e, "postgres connection error");
        }
    });
    _ = client.simple_query("SELECT 1").await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RisingWaveDefaultBatchSettings;

impl SinkBatchSettings for RisingWaveDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(50);
    const MAX_BYTES: Option<usize> = None;
    const TIMEOUT_SECS: f64 = 1.0;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<RisingWaveConfig>();
    }
}
