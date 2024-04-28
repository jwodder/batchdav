use thiserror::Error;
use url::Url;

static USER_AGENT: &str = concat!(
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("CARGO_PKG_REPOSITORY"),
    ")",
);

#[derive(Clone, Debug)]
pub(crate) struct Client {
    base_url: Url,
    inner: reqwest::Client,
}

impl Client {
    pub(crate) fn new(base_url: Url) -> Result<Client, BuildClientError> {
        let inner = reqwest::ClientBuilder::new()
            .user_agent(USER_AGENT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(BuildClientError)?;
        Ok(Client { inner, base_url })
    }

    // Assume `url` has `base_url` as a prefix
    pub(crate) async fn list_directory(&self, url: Url) -> anyhow::Result<DirectoryListing> {
        todo!()
    }

    // Assume `url` has `base_url` as a prefix
    pub(crate) async fn get_file_redirect(&self, url: Url) -> anyhow::Result<Option<Url>> {
        let r = self.inner.head(url).send().await?.error_for_status()?;
        let Some(loc) = r.headers().get(reqwest::header::LOCATION) else {
            return Ok(None);
        };
        let Ok(loc) = loc.to_str() else {
            anyhow::bail!("Could not decode Location header value: {loc:?}");
        };
        match Url::parse(loc) {
            Ok(loc) => Ok(Some(loc)),
            Err(_) => anyhow::bail!("Location header value is not a valid URL: {loc:?}"),
        }
    }
}

#[derive(Debug, Error)]
#[error("failed to initialize HTTP client")]
pub(crate) struct BuildClientError(#[source] reqwest::Error);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryListing {
    pub(crate) directories: Vec<Url>,
    pub(crate) files: Vec<Url>,
}
