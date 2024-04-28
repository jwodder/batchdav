use url::Url;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryListing<T> {
    pub(crate) directories: Vec<T>,
    pub(crate) files: Vec<T>,
}

impl DirectoryListing<String> {
    pub(crate) fn paths_to_urls(self, base_url: &Url) -> DirectoryListing<Url> {
        DirectoryListing {
            directories: self
                .directories
                .into_iter()
                .map(|p| url_plus_path(base_url, &p))
                .collect(),
            files: self
                .files
                .into_iter()
                .map(|p| url_plus_path(base_url, &p))
                .collect(),
        }
    }
}

fn url_plus_path(url: &Url, path: &str) -> Url {
    // TODO: Better error handling:
    url.join(path)
        .expect("href returned from server should be a valid URL path")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(
        "https://www.example.com",
        "/foo/bar/baz",
        "https://www.example.com/foo/bar/baz"
    )]
    #[case(
        "https://www.example.com/quux",
        "/foo/bar/baz",
        "https://www.example.com/foo/bar/baz"
    )]
    #[case(
        "https://www.example.com/quux",
        "https://www.example.com/foo/bar/baz",
        "https://www.example.com/foo/bar/baz"
    )]
    fn test_url_plus_path(#[case] url: Url, #[case] path: &str, #[case] r: Url) {
        assert_eq!(url_plus_path(&url, path), r);
    }
}
