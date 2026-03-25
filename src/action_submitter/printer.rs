use std::{fmt::Debug, marker::PhantomData};

use crate::ActionSubmitter;

#[derive(Debug, Clone)]
pub struct ActionPrinter<A> {
    _phantom: PhantomData<A>,
}

impl<A> Default for ActionPrinter<A> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<A> ActionSubmitter<A> for ActionPrinter<A>
where
    A: Send + Sync + Debug + 'static,
{
    fn submit(&self, a: A) {
        tracing::info!("action: {a:?}");
    }
}
