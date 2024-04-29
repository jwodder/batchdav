mod btn;
mod client;
mod types;
mod xml;
use crate::btn::{BoundedTreeNursery, Spawner};
use crate::client::Client;
use clap::{Parser, Subcommand};
use futures_util::{future::BoxFuture, FutureExt, TryStreamExt};
use statrs::statistics::{Data, Distribution};
use std::fmt;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use url::Url;

/// Traverse WebDAV hierarchies using concurrent tasks
#[derive(Clone, Debug, Eq, Parser, PartialEq)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
enum Command {
    /// Traverse a hierarchy once
    Run {
        /// Do not print details on each request as it's completed
        #[arg(short, long)]
        quiet: bool,

        /// The root URL of the hierarchy
        base_url: Url,

        /// Maximum number of tasks to have active at once
        workers: usize,
    },

    /// Traverse a hierarchy multiple times and summarize the results
    Batch {
        /// Number of traversals to make for each number of workers
        #[arg(short, long, default_value = "10")]
        samples: NonZeroUsize,

        /// The root URL of the hierarchy
        base_url: Url,

        /// Varying worker amounts to run the traversal with
        workers_list: Vec<usize>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Arguments::parse().command {
        Command::Run {
            quiet,
            base_url,
            workers,
        } => {
            let client = Client::new(base_url.clone())?;
            let TraversalReport { requests, elapsed } =
                traverse(client, base_url, workers, quiet).await?;
            println!("Performed {requests} requests in {elapsed:?}");
        }
        Command::Batch {
            samples,
            base_url,
            workers_list,
        } => {
            let client = Client::new(base_url.clone())?;
            let mut stats = Vec::new();
            for workers in workers_list {
                let mut times = Vec::new();
                for i in 1..=samples.into() {
                    let TraversalReport { elapsed, .. } =
                        traverse(client.clone(), base_url.clone(), workers, true).await?;
                    eprintln!("Finished: workers = {workers}, run = {i}, elapsed = {elapsed:?}");
                    times.push(elapsed.as_secs_f64());
                }
                let data = Data::new(times);
                let mean = data
                    .mean()
                    .expect("mean should exist for nonzero number of samples");
                let stddev = data
                    .std_dev()
                    .expect("stddev should exist for nonzero number of samples");
                stats.push((workers, mean, stddev));
            }
            println!("workers,time_mean,time_stddev");
            for (workers, time_mean, time_stddev) in stats {
                println!("{workers},{time_mean},{time_stddev}");
            }
        }
    }
    Ok(())
}

async fn traverse(
    client: Client,
    base_url: Url,
    workers: usize,
    quiet: bool,
) -> anyhow::Result<TraversalReport> {
    let start = Instant::now();
    let mut stream = BoundedTreeNursery::new(workers, move |spawner| {
        process_dir(spawner, client, base_url)
    });
    let mut requests = 0;
    while let Some(r) = stream.try_next().await? {
        requests += 1;
        if !quiet {
            println!("{r}");
        }
    }
    Ok(TraversalReport {
        requests,
        elapsed: start.elapsed(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TraversalReport {
    requests: usize,
    elapsed: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Report {
    Dir(Url),
    File { url: Url, target: Option<Url> },
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Report::Dir(url) => write!(f, "DIR: {url}"),
            Report::File { url, target: None } => write!(f, "FILE: {url} => <NOT A REDIRECT>"),
            Report::File {
                url,
                target: Some(t),
            } => write!(f, "FILE: {url} => {t}"),
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
        let dl = client.list_directory(url.clone()).await?;
        for d in dl.directories {
            let cl2 = client.clone();
            spawner.spawn(move |spawner| Box::pin(process_dir(spawner, cl2, d)));
        }
        for f in dl.files {
            let cl2 = client.clone();
            spawner.spawn(move |_spawner| process_file(cl2, f));
        }
        Ok(Report::Dir(url))
    }
    .boxed()
}

async fn process_file(client: Client, url: Url) -> anyhow::Result<Report> {
    let target = client.get_file_redirect(url.clone()).await?;
    Ok(Report::File { url, target })
}
