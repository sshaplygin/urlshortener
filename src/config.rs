use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Config {
    inner: Arc<ConfigInner>,
}
#[derive(Debug)]
pub struct ConfigInner {
    pub env: Environment,
    pub scheme: String,
    pub domain: String,
}

#[derive(Debug, Clone)]
pub enum Environment {
    Development,
    Production,
}

impl FromStr for Environment {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "development" => Ok(Environment::Development),
            "production" => Ok(Environment::Production),
            _ => Err(()),
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ConfigInner {
                scheme: "http".to_string(),
                domain: "".to_string(),
                env: Environment::Development,
            }),
        }
    }

    pub fn with_domain(self, domain: &str) -> Self {
        let mut inner = self.into_inner();
        inner.domain = domain.to_string();

        Config {
            inner: Arc::new(inner),
        }
    }

    pub fn with_env(self, env: Environment) -> Self {
        let mut inner = self.into_inner();
        inner.env = env;

        Config {
            inner: Arc::new(inner),
        }
    }

    pub fn inner(&self) -> &ConfigInner {
        &self.inner
    }

    pub fn short_url(&self, code: &str) -> String {
        format!(
            "{}://{}/cc/{}",
            self.inner().scheme,
            self.inner().domain,
            code,
        )
    }

    fn into_inner(self) -> ConfigInner {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(arc) => ConfigInner {
                scheme: arc.scheme.clone(),
                domain: arc.domain.clone(),
                env: arc.env.clone(),
            },
        }
    }
}
