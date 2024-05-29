use crate::types::DirectoryListing;
use crate::xml::parse_multistatus;
use indoc::indoc;
use mime::Mime;
use reqwest::Method;
use std::time::{Duration, Instant};
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
    pub(crate) async fn list_directory(
        &self,
        url: Url,
    ) -> anyhow::Result<(DirectoryListing<Url>, Duration)> {
        let start = Instant::now();
        let r = self
            .inner
            .request(self.propfind.clone(), url.clone())
            .header(reqwest::header::CONTENT_TYPE, REQUEST_CONTENT_TYPE)
            .header("Depth", "1")
            .body(REQUEST_BODY)
            .send()
            .await?
            .error_for_status()?;
        let charset = get_charset(&r);
        let resp = r.bytes().await?;
        let elapsed = start.elapsed();
        let mut dl = parse_multistatus(resp, charset)?.paths_to_urls(&self.base_url);
        dl.directories.retain(|u| !is_collection_url(&url, u));
        Ok((dl, elapsed))
    }

    // Assume `url` has `base_url` as a prefix
    pub(crate) async fn get_file_redirect(
        &self,
        url: Url,
    ) -> anyhow::Result<(Option<Url>, Duration)> {
        let start = Instant::now();
        let r = self.inner.head(url).send().await?.error_for_status()?;
        let locvalue = r.headers().get(reqwest::header::LOCATION).cloned();
        let _ = r.bytes().await?;
        let elapsed = start.elapsed();
        let Some(loc) = locvalue else {
            return Ok((None, elapsed));
        };
        let Ok(loc) = loc.to_str() else {
            anyhow::bail!("Could not decode Location header value: {loc:?}");
        };
        match Url::parse(loc) {
            Ok(loc) => Ok((Some(loc), elapsed)),
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

fn get_charset(r: &reqwest::Response) -> Option<String> {
    r.headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<Mime>().ok())
        .and_then(|ct| {
            ct.get_param("charset")
                .map(|charset| charset.as_str().to_owned())
        })
}
