use codecs::encoding::Framer;

use crate::sinks::prelude::*;

use super::{
    batch::RisingWaveBatchSizer,
    request_builder::request_builder,
    service::{RisingWaveRetryLogic, RisingWaveService},
};

pub struct RisingWaveSink {
    pub transformer: Transformer,
    pub encoder: Encoder<Framer>,
    pub service: Svc<RisingWaveService, RisingWaveRetryLogic>,
    pub batch_settings: BatcherSettings,
}

impl RisingWaveSink {
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let batcher_settings = self
            .batch_settings
            .into_item_size_config(RisingWaveBatchSizer {
                encoder: self.encoder.clone(),
            });
        let transformer = self.transformer;
        let mut encoder = self.encoder;

        input
            .batched(batcher_settings)
            .map(|events| request_builder(events, &transformer, &mut encoder))
            .into_driver(self.service)
            .protocol("risingwave")
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl StreamSink<Event> for RisingWaveSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
