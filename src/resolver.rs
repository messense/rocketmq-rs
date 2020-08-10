use std::env;

use dyn_clone::DynClone;

use crate::Error;

const DEFAULT_NAMESRV_ADDR: &'static str = "http://jmenv.tbsite.net:8080/rocketmq/nsaddr";

pub trait NsResolver: DynClone {
    fn resolve(&self) -> Result<Vec<String>, Error>;
    fn description(&self) -> &'static str;
}

dyn_clone::clone_trait_object!(NsResolver);

impl NsResolver for Box<dyn NsResolver> {
    fn resolve(&self) -> Result<Vec<String>, Error> {
        (*self).resolve()
    }

    fn description(&self) -> &'static str {
        (*self).description()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EnvResolver;

impl NsResolver for EnvResolver {
    fn resolve(&self) -> Result<Vec<String>, Error> {
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

impl NsResolver for StaticResolver {
    fn resolve(&self) -> Result<Vec<String>, Error> {
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

impl<T: NsResolver + Clone> NsResolver for PassthroughResolver<T> {
    fn resolve(&self) -> Result<Vec<String>, Error> {
        if self.addrs.is_empty() {
            self.fallback.resolve()
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
    http: reqwest::blocking::Client,
    fallback: EnvResolver,
}

impl HttpResolver {
    pub fn new(instance: String) -> Self {
        Self {
            domain: DEFAULT_NAMESRV_ADDR.to_string(),
            instance,
            http: reqwest::blocking::Client::new(),
            fallback: EnvResolver,
        }
    }

    pub fn with_domain(instance: String, domain: String) -> Self {
        Self {
            domain,
            instance,
            http: reqwest::blocking::Client::new(),
            fallback: EnvResolver,
        }
    }

    pub fn get(&self) -> Result<Vec<String>, Error> {
        let resp = self.http.get(&self.domain).send();
        if let Ok(res) = resp {
            if let Ok(body) = res.text() {
                // TODO: save snapshot to file
                return Ok(body.split(';').map(str::to_string).collect());
            }
        }
        Ok(Vec::new())
    }
}

impl NsResolver for HttpResolver {
    fn resolve(&self) -> Result<Vec<String>, Error> {
        if let Ok(addrs) = self.get() {
            Ok(addrs)
        } else {
            self.fallback.resolve()
        }
    }

    fn description(&self) -> &'static str {
        "http resolver"
    }
}