mod btn;
mod client;
mod show_duration;
mod traverse;
mod types;
mod xml;
use crate::client::Client;
use crate::show_duration::show_duration_as_seconds;
use crate::traverse::{traverse, TraversalReport};
use clap::{Parser, Subcommand};
use statrs::statistics::{Data, Distribution};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
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
        /// Emit a CSV line for each traversal rather than for each set of
        /// traversals per worker quantity
        #[arg(short = 'T', long)]
        per_traversal_stats: bool,

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
            println!("Performed {requests} requests with {workers} workers in {elapsed:?}");
        }
        Command::Batch {
            per_traversal_stats,
            samples,
            base_url,
            workers_list,
        } => {
            let client = Client::new(base_url.clone())?;
            let mut statter = if per_traversal_stats {
                BatchStatter::per_traversal()
            } else {
                BatchStatter::per_workers()
            };
            statter.start();
            for workers in workers_list {
                for _ in 0..samples.get() {
                    let report = traverse(client.clone(), base_url.clone(), workers, true).await?;
                    statter.process(workers, report);
                }
            }
            statter.end();
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
enum BatchStatter {
    PerTraversal,
    PerWorkers {
        worker_runtimes: BTreeMap<usize, Vec<f64>>,
    },
}

impl BatchStatter {
    fn per_traversal() -> Self {
        BatchStatter::PerTraversal
    }

    fn per_workers() -> Self {
        BatchStatter::PerWorkers {
            worker_runtimes: BTreeMap::new(),
        }
    }

    fn start(&self) {
        match self {
            BatchStatter::PerTraversal => println!("workers,requests,elapsed"),
            BatchStatter::PerWorkers { .. } => (),
        }
    }

    fn process(&mut self, workers: usize, TraversalReport { requests, elapsed }: TraversalReport) {
        match self {
            BatchStatter::PerTraversal => {
                println!(
                    "{workers},{requests},{elapsed}",
                    elapsed = show_duration_as_seconds(elapsed),
                );
            }
            BatchStatter::PerWorkers { worker_runtimes } => {
                let timelist = worker_runtimes.entry(workers).or_default();
                timelist.push(elapsed.as_secs_f64());
                let i = timelist.len();
                eprintln!("Finished: workers = {workers}, run = {i}, requests = {requests}, elapsed = {elapsed:?}");
            }
        }
    }

    fn end(self) {
        match self {
            BatchStatter::PerTraversal => (),
            BatchStatter::PerWorkers { worker_runtimes } => {
                println!("workers,time_mean,time_stddev");
                for (workers, runtimes) in worker_runtimes {
                    let data = Data::new(runtimes);
                    let mean = data
                        .mean()
                        .expect("mean should exist for nonzero number of samples");
                    let stddev = data
                        .std_dev()
                        .expect("stddev should exist for nonzero number of samples");
                    println!("{workers},{mean},{stddev}");
                }
            }
        }
    }
}
