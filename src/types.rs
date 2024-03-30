use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use eyre::Result;
use tokio_stream::{Stream, StreamExt};

pub type CollectorStream<'a, E> = Pin<Box<dyn Stream<Item = E> + Send + 'a>>;

#[async_trait]
pub trait Collector<E>: Send + Sync {
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>>;
}

pub trait ActionSubmitter<A>: Send + Sync
where
    A: Send + Sync + Clone + 'static,
{
    fn submit(&self, action: A);
}

#[async_trait]
pub trait Strategy<E, A>: Send + Sync
where
    E: Send + Sync + Clone + 'static,
    A: Send + Sync + Clone + 'static,
{
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: E, submitter: Arc<dyn ActionSubmitter<A>>);
}

pub struct CollectorMap<E, F> {
    collector: Box<dyn Collector<E>>,
    f: F,
}

impl<E, F> CollectorMap<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self { collector, f }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for CollectorMap<E1, F>
where
    E1: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    F: Fn(E1) -> E2 + Send + Sync + Clone + 'static,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.collector.get_event_stream().await?;
        let f = self.f.clone();
        let stream = stream.map(f);
        Ok(Box::pin(stream))
    }
}

pub struct CollectorFilterMap<E, F> {
    collector: Box<dyn Collector<E>>,
    f: F,
}

impl<E, F> CollectorFilterMap<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self { collector, f }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for CollectorFilterMap<E1, F>
where
    E1: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    F: Fn(E1) -> Option<E2> + Send + Sync + Clone + 'static,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.collector.get_event_stream().await?;
        let f = self.f.clone();
        let stream = stream.filter_map(f);
        Ok(Box::pin(stream))
    }
}

#[async_trait]
pub trait Executor<A>: Send + Sync {
    async fn execute(&self, action: A) -> Result<()>;
}

pub struct ExecutorMap<A, F> {
    executor: Box<dyn Executor<A>>,
    f: F,
}

impl<A, F> ExecutorMap<A, F> {
    pub fn new(executor: Box<dyn Executor<A>>, f: F) -> Self {
        Self { executor, f }
    }
}

#[async_trait]
impl<A1, A2, F> Executor<A1> for ExecutorMap<A2, F>
where
    A1: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    F: Fn(A1) -> Option<A2> + Send + Sync + Clone + 'static,
{
    async fn execute(&self, action: A1) -> Result<()> {
        let action = (self.f)(action);
        match action {
            Some(action) => self.executor.execute(action).await,
            None => Ok(()),
        }
    }
}
