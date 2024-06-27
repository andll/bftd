#[cfg(test)]
pub fn enable_test_logging() {
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_thread_names(true)
        .try_init()
        .ok();
}

#[macro_export]
macro_rules! log_byzantine (
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        if true {panic!($($arg)*)} // check that smoke tests do not trigger any byzantine behaviour
        #[cfg(not(debug_assertions))]
        tracing::warn!($($arg)*);
    };
);
