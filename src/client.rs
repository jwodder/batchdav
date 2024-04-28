use crate::types::DirectoryListing;
use crate::xml::parse_propfind_response;
use indoc::indoc;
use reqwest::Method;
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

static REQUEST_CONTENT_TYPE: &str = "text/xml; utf-8";

static REQUEST_BODY: &str = indoc! {r#"
    <?xml version="1.0" encoding="utf-8"?>
    <propfind xmlns="DAV:">
        <prop>
            <resourcetype/>
        </prop>
    </propfind>
"#};

#[derive(Clone, Debug)]
pub(crate) struct Client {
    base_url: Url,
    inner: reqwest::Client,
    propfind: Method,
}

impl Client {
    pub(crate) fn new(base_url: Url) -> Result<Client, BuildClientError> {
        let inner = reqwest::ClientBuilder::new()
            .user_agent(USER_AGENT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(BuildClientError)?;
        Ok(Client {
            inner,
            base_url,
            propfind: "PROPFIND"
                .parse()
                .expect(r#""PROPFIND" should be valid HTTP method"#),
        })
    }

    // Assume `url` has `base_url` as a prefix
    pub(crate) async fn list_directory(&self, url: Url) -> anyhow::Result<DirectoryListing<Url>> {
        let resp = self
            .inner
            .request(self.propfind.clone(), url.clone())
            .header(reqwest::header::CONTENT_TYPE, REQUEST_CONTENT_TYPE)
            .header("Depth", "1")
            .body(REQUEST_BODY)
            .send()
            .await?
            .error_for_status()?
            .text_with_charset("utf-8")
            .await?;
        let mut dl = parse_propfind_response(&resp)?.paths_to_urls(&self.base_url);
        dl.directories.retain(|u| !is_collection_url(&url, u));
        Ok(dl)
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

fn is_collection_url(colurl: &Url, url: &Url) -> bool {
    colurl.as_str().trim_end_matches('/') == url.as_str().trim_end_matches('/')
}
