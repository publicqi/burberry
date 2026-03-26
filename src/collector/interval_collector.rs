use std::marker::PhantomData;
use std::time::Duration;

use crate::types::{Collector, CollectorStream};
use async_trait::async_trait;

pub struct IntervalCollector<T, F> {
    interval: Duration,
    f: F,
    _phantom: PhantomData<T>,
}

impl<T, F> IntervalCollector<T, F>
where
    F: Fn() -> T,
{
    pub fn new(interval: Duration, f: F) -> Self {
        Self {
            interval,
            f,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, F> Collector<T> for IntervalCollector<T, F>
where
    T: Send + Sync + 'static,
    F: Fn() -> T + Send + Sync,
{
    fn name(&self) -> &str {
        "IntervalCollector"
    }

    async fn get_event_stream(&self) -> anyhow::Result<CollectorStream<'_, T>> {
        let stream = async_stream::stream! {
            loop {
                tokio::time::sleep(self.interval).await;
                yield (self.f)();
            }
        };

        Ok(Box::pin(stream))
    }
}
