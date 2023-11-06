use tokio_postgres::{Client, NoTls};

use crate::sinks::prelude::*;

use super::RisingWaveConfig;

pub struct RisingWaveSink {
    #[allow(unused)]
    schema: Option<String>,

    #[allow(unused)]
    table: String,

    #[allow(unused)]
    client: Client,
}

impl RisingWaveSink {
    pub async fn new(config: &RisingWaveConfig) -> crate::Result<Self> {
        let pg_config = config.create_pg_config();

        // Create a tokio_postgres client
        let (client, connection) = pg_config.connect(NoTls).await?;
        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("postgres connection error: {}", e);
            }
        });

        Ok(Self {
            table: config.table.clone(),
            schema: config.schema.clone(),
            client,
        })
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for RisingWaveSink {
    async fn run(
        self: Box<Self>,
        mut input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        while let Some(event) = input.next().await {
            println!("{:?}", event);
        }
        todo!()
    }
}
