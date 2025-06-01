use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Config {
    inner: Arc<ConfigInner>,
}
#[derive(Debug)]
pub struct ConfigInner {
    pub scheme: String,
    pub host: String,
}

impl Config {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ConfigInner {
                scheme: "https".to_string(),
                host: "".to_string(),
            }),
        }
    }

    pub fn with_host(self, host: &str) -> Self {
        let mut inner = self.into_inner();
        inner.host = host.to_string();

        Config {
            inner: Arc::new(inner),
        }
    }

    pub fn inner(&self) -> &ConfigInner {
        &self.inner
    }

    fn into_inner(self) -> ConfigInner {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(arc) => ConfigInner {
                scheme: arc.scheme.clone(),
                host: arc.scheme.clone(),
            },
        }
    }
}
