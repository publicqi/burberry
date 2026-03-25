/// Define an action enum and auto-generate [`Extract`] impls for each variant.
///
/// Each variant must be a single-field tuple variant. The macro generates one
/// `impl Extract<InnerType> for EnumName` per variant, enabling compile-time
/// dispatch via [`Route`] / [`Routes`].
///
/// # Example
///
/// ```ignore
/// burberry::action! {
///     #[derive(Debug)]
///     pub enum Action {
///         Swap(SwapParams),
///         Notify(NotifyParams),
///     }
/// }
/// ```
#[macro_export]
macro_rules! action {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $variant:ident($inner:ty) ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis enum $name {
            $( $variant($inner) ),*
        }

        $(
            impl $crate::Extract<$inner> for $name {
                fn extract(&self) -> Option<&$inner> {
                    #[allow(unreachable_patterns)]
                    match self {
                        $name::$variant(v) => Some(v),
                        _ => None,
                    }
                }
            }
        )*
    };
}

/// Build a typed dispatch tuple from executor → sub-action-type mappings.
///
/// # Example
///
/// ```ignore
/// let dispatch = burberry::dispatch!(
///     SwapExecutor::new() => [SwapParams],
///     AuditLogger::new()  => [SwapParams, NotifyParams],
/// );
/// let engine = Engine::new(dispatch);
/// ```
#[macro_export]
macro_rules! dispatch {
    ( $( $executor:expr => [ $($sub:ty),+ $(,)? ] ),+ $(,)? ) => {
        (
            $( $crate::dispatch!(@entry $executor, $($sub),+ ) ),+,
        )
    };

    // single sub-type → Route
    (@entry $executor:expr, $sub:ty) => {
        $crate::Route::<_, $sub>::new($executor)
    };

    // multiple sub-types → Routes
    (@entry $executor:expr, $($sub:ty),+) => {
        $crate::Routes::<_, ($($sub,)+)>::new($executor)
    };
}

#[macro_export]
macro_rules! map_boxed_collector {
    ($collector: expr, $variant: path) => {
        Box::new($crate::CollectorMap::new($collector, $variant))
    };
}

#[macro_export]
macro_rules! map_collector {
    ($collector: expr, $variant: path) => {
        $crate::map_boxed_collector!(Box::new($collector), $variant)
    };
}

#[macro_export]
macro_rules! submit_action {
    ($submitter: expr, $variant: path, $action: expr) => {
        $submitter.submit($variant($action));
    };
}
