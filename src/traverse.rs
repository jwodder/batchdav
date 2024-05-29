use crate::btn::{BoundedTreeNursery, Spawner};
use crate::client::Client;
use futures_util::{future::BoxFuture, FutureExt, TryStreamExt};
use serde::Serialize;
use std::fmt;
use std::time::{Duration, Instant};
use url::Url;

pub(crate) async fn traverse(
    client: Client,
    base_url: Url,
    workers: usize,
    quiet: bool,
) -> anyhow::Result<TraversalReport> {
    let start = Instant::now();
    let mut stream = BoundedTreeNursery::new(workers, move |spawner| {
        process_dir(spawner, client, base_url)
    });
    let mut directory_request_times = Vec::new();
    let mut file_request_times = Vec::new();
    while let Some(r) = stream.try_next().await? {
        if !quiet {
            println!("{r}");
        }
        match r {
            Report::Dir { elapsed, .. } => directory_request_times.push(elapsed),
            Report::File { elapsed, .. } => file_request_times.push(elapsed),
        }
    }
    Ok(TraversalReport {
        workers,
        directory_request_times,
        file_request_times,
        overall_time: start.elapsed(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct TraversalReport {
    pub(crate) workers: usize,
    pub(crate) directory_request_times: Vec<Duration>,
    pub(crate) file_request_times: Vec<Duration>,
    pub(crate) overall_time: Duration,
}

impl TraversalReport {
    pub(crate) fn requests(&self) -> usize {
        self.directory_request_times
            .len()
            .saturating_add(self.file_request_times.len())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Report {
    Dir {
        url: Url,
        elapsed: Duration,
    },
    File {
        url: Url,
        elapsed: Duration,
        target: Option<Url>,
    },
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Report::Dir { url, elapsed } => write!(f, "DIR: {url} ({elapsed:?})"),
            Report::File {
                url,
                elapsed,
                target: None,
            } => write!(f, "FILE: {url} => <NOT A REDIRECT> ({elapsed:?})"),
            Report::File {
                url,
                elapsed,
                target: Some(t),
            } => write!(f, "FILE: {url} => {t} ({elapsed:?})"),
        }
    }
}

fn process_dir(
    spawner: Spawner<anyhow::Result<Report>>,
    client: Client,
    url: Url,
) -> BoxFuture<'static, anyhow::Result<Report>> {
    // We need to return a boxed Future in order to be able to call
    // `process_dir()` inside itself.
    async move {
        let (dl, elapsed) = client.list_directory(url.clone()).await?;
        for d in dl.directories {
            let cl2 = client.clone();
            spawner.spawn(move |spawner| process_dir(spawner, cl2, d));
        }
        for f in dl.files {
            let cl2 = client.clone();
            spawner.spawn(move |_spawner| process_file(cl2, f));
        }
        Ok(Report::Dir { url, elapsed })
    }
    .boxed()
}

async fn process_file(client: Client, url: Url) -> anyhow::Result<Report> {
    let (target, elapsed) = client.get_file_redirect(url.clone()).await?;
    Ok(Report::File {
        url,
        elapsed,
        target,
    })
}
