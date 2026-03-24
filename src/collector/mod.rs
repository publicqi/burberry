#[cfg(feature = "ethereum")]
pub mod ethereum;

mod interval_collector;
pub use interval_collector::IntervalCollector;
