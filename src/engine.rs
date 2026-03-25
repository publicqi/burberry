use std::sync::Arc;

use crate::{
    action_submitter::DispatchSubmitter,
    types::{Collector, Dispatch, Strategy},
};
use anyhow::Context as _;
use futures::StreamExt;
use tokio::{
    sync::broadcast::{self, error::RecvError},
    task::JoinSet,
};
use tracing::{debug, error, warn};

pub struct Engine<E, A, D> {
    collectors: Vec<Box<dyn Collector<E>>>,
    strategies: Vec<Box<dyn Strategy<E, A>>>,
    dispatch: D,

    event_channel_capacity: usize,
}

impl<E, A, D> Engine<E, A, D> {
    pub fn new(dispatch: D) -> Self {
        Self {
            collectors: vec![],
            strategies: vec![],
            dispatch,
            event_channel_capacity: 512,
        }
    }

    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    pub fn strategy_count(&self) -> usize {
        self.strategies.len()
    }
}

impl<E, A, D> Engine<E, A, D>
where
    E: Send + Sync + Clone + 'static,
    A: Send + Sync + 'static,
    D: Dispatch<A> + 'static,
{
    pub fn add_collector(&mut self, collector: Box<dyn Collector<E>>) {
        self.collectors.push(collector);
    }

    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy<E, A>>) {
        self.strategies.push(strategy);
    }

    pub async fn run_and_join(self) -> Result<(), Box<dyn std::error::Error>> {
        let mut js = self.run().await?;

        while let Some(event) = js.join_next().await {
            if let Err(err) = event {
                error!("task terminated unexpectedly: {err:#}");
            }
        }

        Ok(())
    }

    pub async fn run(self) -> Result<JoinSet<()>, Box<dyn std::error::Error>> {
        let (event_sender, _) = broadcast::channel(self.event_channel_capacity);

        let mut set = JoinSet::new();

        if self.collectors.is_empty() {
            return Err("no collectors".into());
        }

        if self.strategies.is_empty() {
            return Err("no strategies".into());
        }

        let dispatch = Arc::new(self.dispatch);
        let submitter = Arc::new(DispatchSubmitter::new(dispatch));

        // Spawn strategy tasks.
        for mut strategy in self.strategies {
            let mut event_receiver = event_sender.subscribe();
            let submitter = submitter.clone();

            strategy
                .sync_state(submitter.clone())
                .await
                .context("fail to sync state")?;

            set.spawn(async move {
                debug!(name = strategy.name(), "starting strategy... ");

                loop {
                    match event_receiver.recv().await {
                        Ok(event) => strategy.process_event(event, submitter.clone()).await,
                        Err(RecvError::Closed) => {
                            error!(name = strategy.name(), "event channel closed!");
                            break;
                        }
                        Err(RecvError::Lagged(num)) => {
                            warn!(name = strategy.name(), "event channel lagged by {num}")
                        }
                    }
                }
            });
        }

        // Spawn collector tasks.
        for collector in self.collectors {
            let event_sender = event_sender.clone();

            set.spawn(async move {
                debug!(name = collector.name(), "starting collector... ");
                let mut event_stream = collector.get_event_stream().await.unwrap();

                while let Some(event) = event_stream.next().await {
                    if let Err(e) = event_sender.send(event) {
                        error!(name = collector.name(), "error sending event: {e:#}");
                    }
                }

                error!(name = collector.name(), "event stream ended!");
            });
        }

        Ok(set)
    }
}
