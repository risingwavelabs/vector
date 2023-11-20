use std::collections::HashMap;

use codecs::{JsonSerializerConfig, TextSerializerConfig};
use vector_common::request_metadata::GroupedCountByteSize;
use vector_core::{config::log_schema, event::LogEvent};

use crate::{
    codecs::{Encoder, Transformer},
    sinks::risingwave::request_builder::encode_event,
};

#[test]
fn risingwave_log_event_json() {
    let msg = "hello_world".to_owned();
    let evt = LogEvent::from(msg.clone());
    let mut byte_size = GroupedCountByteSize::new_untagged();
    let result = encode_event(
        evt.into(),
        &Default::default(),
        &mut Encoder::<()>::new(JsonSerializerConfig::default().build().into()),
        &mut byte_size,
    )
    .unwrap()
    .value;
    let map: HashMap<String, String> = serde_json::from_slice(&result[..]).unwrap();
    assert_eq!(msg, map[&log_schema().message_key().unwrap().to_string()]);
}

#[test]
fn risingwave_log_event_text() {
    let msg = "hello_world".to_owned();
    let evt = LogEvent::from(msg.clone());
    let mut byte_size = GroupedCountByteSize::new_untagged();
    let event = encode_event(
        evt.into(),
        &Default::default(),
        &mut Encoder::<()>::new(TextSerializerConfig::default().build().into()),
        &mut byte_size,
    )
    .unwrap()
    .value;
    assert_eq!(event, msg.as_bytes());
}

#[test]
fn risingwave_encode_event() {
    let msg = "hello_world";
    let mut evt = LogEvent::from(msg);
    let mut byte_size = GroupedCountByteSize::new_untagged();
    evt.insert("key", "value");

    let result = encode_event(
        evt.into(),
        &Transformer::new(None, Some(vec!["key".into()]), None).unwrap(),
        &mut Encoder::<()>::new(JsonSerializerConfig::default().build().into()),
        &mut byte_size,
    )
    .unwrap()
    .value;

    let map: HashMap<String, String> = serde_json::from_slice(&result[..]).unwrap();
    assert!(!map.contains_key("key"));
}
