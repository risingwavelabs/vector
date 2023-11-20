use vector_core::{
    event::Event, stream::batcher::limiter::ItemBatchSize, ByteSizeOf, EstimatedJsonEncodedSizeOf,
};

use crate::codecs::Encoder;

#[derive(Default)]
pub(super) struct RisingWaveBatchSizer {
    pub(super) encoder: Encoder<()>,
}

impl RisingWaveBatchSizer {
    pub(super) fn estimated_size_of(&self, event: &Event) -> usize {
        match self.encoder.serializer() {
            codecs::encoding::Serializer::Json(_) | codecs::encoding::Serializer::NativeJson(_) => {
                event.estimated_json_encoded_size_of().get()
            }
            _ => event.size_of(),
        }
    }
}

impl ItemBatchSize<Event> for RisingWaveBatchSizer {
    fn size(&self, event: &Event) -> usize {
        self.estimated_size_of(event)
    }
}
