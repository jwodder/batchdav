mod client;
mod show_duration;
mod traverse;
mod types;
mod worker_nursery;
mod xml;
use crate::client::Client;
use crate::show_duration::show_duration_as_seconds;
use crate::traverse::{traverse, TraversalReport};
use anyhow::Context;
use clap::{Parser, Subcommand};
use serde::Serialize;
use statrs::statistics::{Data, Distribution};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use time::OffsetDateTime;
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
        workers: NonZeroUsize,
    },

    /// Traverse a hierarchy multiple times and summarize the results
    Batch {
        #[arg(short = 'J', long, conflicts_with = "per_traversal_stats")]
        json_file: Option<PathBuf>,

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
        workers_list: Vec<NonZeroUsize>,
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
            let report = traverse(client, base_url, workers, quiet).await?;
            println!(
                "Performed {} requests with {} workers in {:?}",
                report.requests(),
                report.workers,
                report.overall_time
            );
        }
        Command::Batch {
            json_file,
            per_traversal_stats,
            samples,
            base_url,
            workers_list,
        } => {
            let client = Client::new(base_url.clone())?;
            let mut statter = if let Some(path) = json_file {
                StatManager::json_file(path, base_url.clone())
            } else if per_traversal_stats {
                StatManager::per_traversal()
            } else {
                StatManager::per_workers()
            };
            statter.start();
            for workers in workers_list {
                for _ in 0..samples.get() {
                    let report = traverse(client.clone(), base_url.clone(), workers, true).await?;
                    statter.process(report);
                }
            }
            statter.end()?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
enum StatManager {
    JsonFile {
        outfile: PathBuf,
        data: StatReport,
    },
    PerTraversal,
    PerWorkers {
        worker_runtimes: BTreeMap<NonZeroUsize, Vec<f64>>,
    },
}

impl StatManager {
    fn json_file(outfile: PathBuf, base_url: Url) -> Self {
        StatManager::JsonFile {
            outfile,
            data: StatReport::new(base_url),
        }
    }

    fn per_traversal() -> Self {
        StatManager::PerTraversal
    }

    fn per_workers() -> Self {
        StatManager::PerWorkers {
            worker_runtimes: BTreeMap::new(),
        }
    }

    fn start(&mut self) {
        match self {
            StatManager::JsonFile { data, .. } => data.start_time = Some(OffsetDateTime::now_utc()),
            StatManager::PerTraversal => println!("workers,requests,elapsed"),
            StatManager::PerWorkers { .. } => (),
        }
    }

    fn process(&mut self, report: TraversalReport) {
        match self {
            StatManager::JsonFile { data, .. } => {
                eprintln!(
                    "Finished: workers = {}, requests = {}, elapsed = {:?}",
                    report.workers,
                    report.requests(),
                    report.overall_time
                );
                data.traversals.push(report);
            }
            StatManager::PerTraversal => {
                println!(
                    "{},{},{}",
                    report.workers,
                    report.requests(),
                    show_duration_as_seconds(report.overall_time),
                );
            }
            StatManager::PerWorkers { worker_runtimes } => {
                let workers = report.workers;
                let elapsed = report.overall_time;
                let requests = report.requests();
                let timelist = worker_runtimes.entry(workers).or_default();
                timelist.push(elapsed.as_secs_f64());
                let i = timelist.len();
                eprintln!("Finished: workers = {workers}, run = {i}, requests = {requests}, elapsed = {elapsed:?}");
            }
        }
    }

    fn end(self) -> anyhow::Result<()> {
        match self {
            StatManager::JsonFile { outfile, mut data } => {
                data.end_time = Some(OffsetDateTime::now_utc());
                let mut fp =
                    BufWriter::new(File::create(outfile).context("failed to open JSON outfile")?);
                serde_json::to_writer_pretty(&mut fp, &data)
                    .context("failed to dump JSON to file")?;
                fp.write_all(b"\n")
                    .context("failed to write final newline to JSON outfile")?;
                fp.flush().context("failed to flush JSON outfile")?;
            }
            StatManager::PerTraversal => (),
            StatManager::PerWorkers { worker_runtimes } => {
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
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct StatReport {
    #[serde(with = "time::serde::rfc3339::option")]
    start_time: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    end_time: Option<OffsetDateTime>,
    base_url: Url,
    traversals: Vec<TraversalReport>,
}

impl StatReport {
    fn new(base_url: Url) -> Self {
        StatReport {
            start_time: None,
            end_time: None,
            base_url,
            traversals: Vec::new(),
        }
    }
}
