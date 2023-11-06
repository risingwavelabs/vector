use tokio_postgres::{Config, NoTls};

use crate::sinks::prelude::*;

use super::sink::RisingWaveSink;

/// Configuration for the `risingwave` sink.
#[configurable_component(sink("risingwave", "Deliver log data to a RisingWave database."))]
#[derive(Clone, Debug, Default)]
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
        let default = Self {
            host: "localhost".to_string(),
            port: 4566,
            database: "dev".to_string(),
            user: "root".to_string(),
            ..Default::default()
        };
        toml::value::Value::try_from(default).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "risingwave")]
impl SinkConfig for RisingWaveConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let sink = RisingWaveSink::new(self).await?;

        // Healthcheck could be a simple query to the Risingwave database
        let healthcheck = healthcheck(self.create_pg_config()).boxed();

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

async fn healthcheck(pg_config: Config) -> crate::Result<()> {
    // TODO: Reuse the connection and avoid reconnecting every health check.
    let (client, connection) = pg_config.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(?e, "postgres connection error");
        }
    });
    let _ = client.simple_query("SELECT 1").await?;
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
