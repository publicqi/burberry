use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use alloy::{
    primitives::B256,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::{eth::Transaction, Header},
};
use burberry::{
    collector::ethereum::{BlockCollector, MempoolCollector},
    dispatch, map_collector, submit_action, ActionSubmitter, Engine, Executor, Strategy,
};

#[tokio::main]
async fn main() {
    let ws = WsConnect::new("wss://eth.merkle.io");
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .expect("fail to create ws provider");

    let provider: Arc<dyn Provider<_>> = Arc::new(provider);

    let mempool_collector = MempoolCollector::new(Arc::clone(&provider));
    let block_collector = BlockCollector::new(Arc::clone(&provider));

    let mut engine = Engine::new(dispatch!(
        EchoExecutor::<u64>::default()  => [u64],
        EchoExecutor::<B256>::default() => [B256],
    ));

    engine.add_collector(map_collector!(mempool_collector, Event::Transaction));
    engine.add_collector(map_collector!(block_collector, Event::Block));

    engine.add_strategy(Box::new(EchoStrategy));

    engine.run_and_join().await.unwrap()
}

pub struct EchoStrategy;

#[async_trait::async_trait]
impl Strategy<Event, Action> for EchoStrategy {
    async fn process_event(&mut self, event: Event, submitter: Arc<dyn ActionSubmitter<Action>>) {
        match event {
            Event::Block(block) => {
                submit_action!(submitter, Action::EchoBlock, block.number);
            }
            Event::Transaction(tx) => {
                submit_action!(submitter, Action::EchoTransaction, *tx.inner.tx_hash());
            }
        }
    }
}

#[derive(Default)]
pub struct EchoExecutor<T>(PhantomData<T>);

#[async_trait::async_trait]
impl<T: Debug + Send + Sync + 'static> Executor<T> for EchoExecutor<T> {
    async fn execute(&self, action: &T) -> anyhow::Result<()> {
        println!("action: {action:?}");
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum Event {
    Block(Header),
    Transaction(Transaction),
}

burberry::action! {
    #[derive(Debug)]
    pub enum Action {
        EchoBlock(u64),
        EchoTransaction(B256),
    }
}
