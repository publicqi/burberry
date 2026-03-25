mod map;
mod printer;

#[cfg(feature = "telegram")]
mod telegram;

use std::sync::Arc;

pub use map::ActionSubmitterMap;
pub use printer::ActionPrinter;

#[cfg(feature = "telegram")]
pub use telegram::TelegramSubmitter;

use crate::{ActionSubmitter, Dispatch};

/// Submits actions by spawning a tokio task that dispatches via the typed
/// [`Dispatch`] pipeline. No channels, no `Clone` on `A`.
pub struct DispatchSubmitter<D> {
    dispatch: Arc<D>,
}

impl<D> Clone for DispatchSubmitter<D> {
    fn clone(&self) -> Self {
        Self {
            dispatch: Arc::clone(&self.dispatch),
        }
    }
}

impl<D> DispatchSubmitter<D> {
    pub fn new(dispatch: Arc<D>) -> Self {
        Self { dispatch }
    }
}

impl<A, D> ActionSubmitter<A> for DispatchSubmitter<D>
where
    A: Send + Sync + 'static,
    D: Dispatch<A> + 'static,
{
    fn submit(&self, action: A) {
        let dispatch = Arc::clone(&self.dispatch);
        tokio::spawn(async move {
            dispatch.dispatch(&action).await;
        });
    }
}
