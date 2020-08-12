use std::env;

use async_trait::async_trait;

use crate::Error;

const DEFAULT_NAMESRV_ADDR: &'static str = "http://jmenv.tbsite.net:8080/rocketmq/nsaddr";

#[async_trait]
pub trait NsResolver {
    async fn resolve(&self) -> Result<Vec<String>, Error>;
    fn description(&self) -> &'static str;
}

#[derive(Debug, Clone)]
pub enum Resolver {
    Env(EnvResolver),
    Static(StaticResolver),
    PassthroughHttp(PassthroughResolver<HttpResolver>),
    Http(HttpResolver),
}

#[async_trait]
impl NsResolver for Resolver {
    async fn resolve(&self) -> Result<Vec<String>, Error> {
        Ok(match self {
            Resolver::Env(inner) => inner.resolve().await?,
            Resolver::Static(inner) => inner.resolve().await?,
            Resolver::PassthroughHttp(inner) => inner.resolve().await?,
            Resolver::Http(inner) => inner.resolve().await?,
        })
    }

    fn description(&self) -> &'static str {
        match self {
            Resolver::Env(inner) => inner.description(),
            Resolver::Static(inner) => inner.description(),
            Resolver::PassthroughHttp(inner) => inner.description(),
            Resolver::Http(inner) => inner.description(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EnvResolver;

#[async_trait]
impl NsResolver for EnvResolver {
    async fn resolve(&self) -> Result<Vec<String>, Error> {
        Ok(env::var("NAMESRV_ADDR")
            .map(|s| s.split(';').map(str::to_string).collect())
            .unwrap_or_default())
    }

    fn description(&self) -> &'static str {
        "envvar resolver"
    }
}

#[derive(Debug, Clone)]
pub struct StaticResolver {
    addrs: Vec<String>,
}

impl StaticResolver {
    pub fn new(addrs: Vec<String>) -> Self {
        Self { addrs }
    }
}

#[async_trait]
impl NsResolver for StaticResolver {
    async fn resolve(&self) -> Result<Vec<String>, Error> {
        Ok(self.addrs.clone())
    }

    fn description(&self) -> &'static str {
        "static resolver"
    }
}

#[derive(Debug, Clone)]
pub struct PassthroughResolver<T: NsResolver> {
    addrs: Vec<String>,
    fallback: T,
}

impl<T: NsResolver> PassthroughResolver<T> {
    pub fn new(addrs: Vec<String>, fallback: T) -> Self {
        Self { addrs, fallback }
    }
}

#[async_trait]
impl<T: NsResolver + Clone + Send + Sync> NsResolver for PassthroughResolver<T> {
    async fn resolve(&self) -> Result<Vec<String>, Error> {
        if self.addrs.is_empty() {
            Ok(self.fallback.resolve().await?)
        } else {
            Ok(self.addrs.clone())
        }
    }

    fn description(&self) -> &'static str {
        "passthrough resolver"
    }
}

#[derive(Debug, Clone)]
pub struct HttpResolver {
    domain: String,
    instance: String,
    http: reqwest::Client,
    fallback: EnvResolver,
}

impl HttpResolver {
    pub fn new(instance: String) -> Self {
        Self {
            domain: DEFAULT_NAMESRV_ADDR.to_string(),
            instance,
            http: reqwest::Client::new(),
            fallback: EnvResolver,
        }
    }

    pub fn with_domain(instance: String, domain: String) -> Self {
        Self {
            domain,
            instance,
            http: reqwest::Client::new(),
            fallback: EnvResolver,
        }
    }

    async fn get(&self) -> Result<Vec<String>, Error> {
        let resp = self.http.get(&self.domain).send().await;
        if let Ok(res) = resp {
            if let Ok(body) = res.text().await {
                // TODO: save snapshot to file
                return Ok(body.split(';').map(str::to_string).collect());
            }
        }
        Ok(Vec::new())
    }
}

#[async_trait]
impl NsResolver for HttpResolver {
    async fn resolve(&self) -> Result<Vec<String>, Error> {
        if let Ok(addrs) = self.get().await {
            Ok(addrs)
        } else {
            Ok(self.fallback.resolve().await?)
        }
    }

    fn description(&self) -> &'static str {
        "http resolver"
    }
}
