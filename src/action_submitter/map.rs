use std::marker::PhantomData;

use crate::ActionSubmitter;

pub struct ActionSubmitterMap<A1, A2, F> {
    submitter: Box<dyn ActionSubmitter<A2>>,
    f: F,
    _phantom: PhantomData<A1>,
}

impl<A1, A2, F> ActionSubmitterMap<A1, A2, F> {
    pub fn new(submitter: Box<dyn ActionSubmitter<A2>>, f: F) -> Self {
        Self {
            submitter,
            f,
            _phantom: PhantomData,
        }
    }
}

impl<A1, A2, F> ActionSubmitter<A1> for ActionSubmitterMap<A1, A2, F>
where
    A1: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    F: Fn(A1) -> Option<A2> + Send + Sync + 'static,
{
    fn submit(&self, a: A1) {
        if let Some(a) = (self.f)(a) {
            self.submitter.submit(a);
        }
    }
}
