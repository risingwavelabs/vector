use bytes::BytesMut;
use tokio_util::codec::Encoder as _;

use crate::{
    codecs::{Encoder, Transformer},
    sinks::prelude::*,
};

use super::service::{RisingWaveBody, RisingWaveRequest, RisingWaveRequestMetadata};

pub fn request_builder(
    mut events: Vec<Event>,
    transformer: &Transformer,
    encoder: &mut Encoder<()>,
) -> RisingWaveRequest {
    let finalizers = events.take_finalizers();
    let builder = RequestMetadataBuilder::from_events(&events);
    let encoded = encode_events(events, transformer, encoder);
    let request_metadata = builder.build(&encoded);
    let metadata = RisingWaveRequestMetadata { finalizers };

    RisingWaveRequest {
        body: encoded.into_payload(),
        metadata,
        request_metadata,
    }
}

fn encode_events(
    events: Vec<Event>,
    transformer: &Transformer,
    encoder: &mut Encoder<()>,
) -> EncodeResult<Vec<RisingWaveBody>> {
    let mut byte_size = telemetry().create_request_count_byte_size();
    let request = events
        .into_iter()
        .filter_map(|event| encode_event(event, transformer, encoder, &mut byte_size))
        .collect::<Vec<_>>();

    let uncompressed_byte_size = request.iter().map(|_event| 1).sum();

    EncodeResult {
        payload: request,
        uncompressed_byte_size,
        transformed_json_size: byte_size,
        compressed_byte_size: None,
    }
}

fn encode_event(
    mut event: Event,
    transformer: &Transformer,
    encoder: &mut Encoder<()>,
    byte_size: &mut GroupedCountByteSize,
) -> Option<RisingWaveBody> {
    transformer.transform(&mut event);
    byte_size.add_event(&event, event.estimated_json_encoded_size_of());

    let mut bytes = BytesMut::new();

    let data_type = match event {
        Event::Log(_) => DataType::Log,
        Event::Metric(_) => DataType::Metric,
        Event::Trace(_) => DataType::Trace,
    };
    let source_id = event.source_id().cloned();

    _ = encoder.encode(event, &mut bytes);

    let value = bytes.freeze();

    let body = RisingWaveBody {
        value,
        data_type,
        source_id,
    };

    Some(body)
}
