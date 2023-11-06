//! The RisingWave [risingwave::sink::RisingWaveSink]
//!
//! This module contains the [risingwave::sink::RisingWaveSink] instance that is responsible for
//! taking a stream of [risingwave::event::Event] instances and forwarding them to RisingWave.
//!
//! Events are sent to RisingWave using the Postgres binary wire protocol, using the INSERT
//! statement.
//!
//! This sink currently supports logs and has the potential to support metrics and traces in the future.

mod config;
#[cfg(all(test, feature = "risingwave-integration-tests"))]
mod integration_tests;
mod sink;
pub use self::config::RisingWaveConfig;
