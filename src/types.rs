use std::{marker::PhantomData, pin::Pin, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use futures::{Stream, StreamExt};

// ---------------------------------------------------------------------------
// Collector
// ---------------------------------------------------------------------------

pub type CollectorStream<'a, E> = Pin<Box<dyn Stream<Item = E> + Send + 'a>>;

#[async_trait]
pub trait Collector<E>: Send + Sync {
    fn name(&self) -> &str {
        "Unnamed"
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>>;
}

pub struct CollectorMap<E, F> {
    inner: Box<dyn Collector<E>>,
    f: F,
}

impl<E, F> CollectorMap<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self {
            inner: collector,
            f,
        }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for CollectorMap<E1, F>
where
    E1: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    F: Fn(E1) -> E2 + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.inner.get_event_stream().await?;
        let f = self.f.clone();
        let stream = stream.map(f);
        Ok(Box::pin(stream))
    }
}

pub struct CollectorFilterMap<E, F> {
    inner: Box<dyn Collector<E>>,
    f: F,
}

impl<E, F> CollectorFilterMap<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self {
            inner: collector,
            f,
        }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for CollectorFilterMap<E1, F>
where
    E1: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    F: Fn(E1) -> Option<E2> + Send + Sync + Clone + Copy + 'static,
{
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.inner.get_event_stream().await?;
        let f = self.f;
        let stream = stream.filter_map(move |v| async move { f(v) });
        Ok(Box::pin(stream))
    }
}

// ---------------------------------------------------------------------------
// ActionSubmitter
// ---------------------------------------------------------------------------

pub trait ActionSubmitter<A>: Send + Sync
where
    A: Send + Sync + 'static,
{
    fn submit(&self, action: A);
}

// ---------------------------------------------------------------------------
// Strategy
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Strategy<E, A>: Send + Sync
where
    E: Send + Sync + Clone + 'static,
    A: Send + Sync + 'static,
{
    fn name(&self) -> &str {
        "Unnamed"
    }

    async fn sync_state(&mut self, _submitter: Arc<dyn ActionSubmitter<A>>) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: E, submitter: Arc<dyn ActionSubmitter<A>>);
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Executor<A: Send + Sync>: Send + Sync {
    fn name(&self) -> &str {
        "Unnamed"
    }

    async fn execute(&self, action: &A) -> Result<()>;
}

#[async_trait]
impl<A, E> Executor<A> for Arc<E>
where
    A: Send + Sync + 'static,
    E: Executor<A> + 'static,
{
    fn name(&self) -> &str {
        E::name(self)
    }

    async fn execute(&self, action: &A) -> Result<()> {
        E::execute(self, action).await
    }
}

// ---------------------------------------------------------------------------
// Extract
// ---------------------------------------------------------------------------

pub trait Extract<Sub> {
    fn extract(&self) -> Option<&Sub>;
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Dispatch<A: Send + Sync>: Send + Sync {
    async fn dispatch(&self, action: &A);
}

#[async_trait]
impl<A: Send + Sync + 'static> Dispatch<A> for () {
    async fn dispatch(&self, _action: &A) {}
}

// ---------------------------------------------------------------------------
// Route
// ---------------------------------------------------------------------------

pub struct Route<E, Sub> {
    executor: E,
    _sub: PhantomData<Sub>,
}

impl<E, Sub> Route<E, Sub> {
    pub fn new(executor: E) -> Self {
        Self {
            executor,
            _sub: PhantomData,
        }
    }
}

#[async_trait]
impl<A, E, Sub> Dispatch<A> for Route<E, Sub>
where
    A: Extract<Sub> + Send + Sync + 'static,
    Sub: Send + Sync + 'static,
    E: Executor<Sub> + 'static,
{
    async fn dispatch(&self, action: &A) {
        if let Some(sub) = action.extract() {
            if let Err(e) = self.executor.execute(sub).await {
                tracing::error!(name = self.executor.name(), "error executing action: {e}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

pub struct Routes<E, Subs> {
    executor: E,
    _subs: PhantomData<Subs>,
}

impl<E, Subs> Routes<E, Subs> {
    pub fn new(executor: E) -> Self {
        Self {
            executor,
            _subs: PhantomData,
        }
    }
}

macro_rules! impl_routes {
    ($($Sub:ident),+) => {
        #[async_trait]
        impl<A, E, $($Sub),+> Dispatch<A> for Routes<E, ($($Sub,)+)>
        where
            A: $( Extract<$Sub> + )+ Send + Sync + 'static,
            $( $Sub: Send + Sync + 'static, )+
            E: $( Executor<$Sub> + )+ Send + Sync + 'static,
        {
            async fn dispatch(&self, action: &A) {
                $(
                    if let Some(sub) = <A as Extract<$Sub>>::extract(action) {
                        if let Err(e) = Executor::<$Sub>::execute(&self.executor, sub).await {
                            tracing::error!("error executing action: {e}");
                        }
                    }
                )+
            }
        }
    };
}

impl_routes!(S0);
impl_routes!(S0, S1);
impl_routes!(S0, S1, S2);
impl_routes!(S0, S1, S2, S3);
impl_routes!(S0, S1, S2, S3, S4);
impl_routes!(S0, S1, S2, S3, S4, S5);
impl_routes!(S0, S1, S2, S3, S4, S5, S6);
impl_routes!(S0, S1, S2, S3, S4, S5, S6, S7);

// ---------------------------------------------------------------------------
// Dispatch for tuples
// ---------------------------------------------------------------------------

macro_rules! impl_dispatch_tuple {
    ($($idx:tt : $T:ident),+) => {
        #[async_trait]
        impl<A: Send + Sync + 'static, $($T: Dispatch<A> + 'static),+> Dispatch<A> for ($($T,)+) {
            async fn dispatch(&self, action: &A) {
                tokio::join!(
                    $(self.$idx.dispatch(action)),+
                );
            }
        }
    };
}

impl_dispatch_tuple!(0: T0);
impl_dispatch_tuple!(0: T0, 1: T1);
impl_dispatch_tuple!(0: T0, 1: T1, 2: T2);
impl_dispatch_tuple!(0: T0, 1: T1, 2: T2, 3: T3);
impl_dispatch_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4);
impl_dispatch_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5);
impl_dispatch_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6);
impl_dispatch_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7);
