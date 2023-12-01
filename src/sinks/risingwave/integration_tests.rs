use std::sync::Arc;

use codecs::TextSerializerConfig;
use tokio_postgres::NoTls;

use crate::sinks::{risingwave::RisingWaveConfig, util::BatchConfig};

#[tokio::test]
async fn healthcheck() {
    crate::test_util::trace_init();

    let config = RisingWaveConfig {
        host: "localhost".to_string(),
        port: 4566,
        database: "dev".to_string(),
        schema: None,
        table: "t".to_string(),
        user: "dev".to_string(),
        password: None,
        request: Default::default(),
        encoding: TextSerializerConfig::default().into(),
        batch: BatchConfig::default(),
        acknowledgements: Default::default(),
    };

    let pg_config = config.create_pg_config();
    let (client, connection) = pg_config.connect(NoTls).await.unwrap();
    let client = Arc::new(client);

    assert!(super::config::healthcheck(client, connection).await.is_ok());
}
