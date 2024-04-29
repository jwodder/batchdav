mod btn;
mod client;
mod types;
mod xml;
use crate::btn::{BoundedTreeNursery, Spawner};
use crate::client::Client;
use clap::{Parser, Subcommand};
use futures_util::{future::BoxFuture, FutureExt, TryStreamExt};
use std::time::Instant;
use url::Url;

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
enum Command {
    Run {
        #[arg(short, long)]
        quiet: bool,

        base_url: Url,

        workers: usize,
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
            let start = Instant::now();
            let mut stream = BoundedTreeNursery::new(workers, move |spawner| async move {
                process_dir(spawner, client, base_url).await
            })
            .await;
            let mut requests = 0;
            while let Some(r) = stream.try_next().await? {
                requests += 1;
                if !quiet {
                    if let Report::File { url, target } = r {
                        println!(
                            "{} -> {}",
                            url,
                            target.as_ref().map_or("<NOT A REDIRECT>", Url::as_str)
                        );
                    }
                }
            }
            let elapsed = start.elapsed();
            println!("Performed {requests} requests in {elapsed:?}");
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Report {
    Dir,
    File { url: Url, target: Option<Url> },
}

fn process_dir(
    spawner: Spawner<anyhow::Result<Report>>,
    client: Client,
    url: Url,
) -> BoxFuture<'static, anyhow::Result<Report>> {
    async move {
        let dl = client.list_directory(url).await?;
        for d in dl.directories {
            let cl2 = client.clone();
            spawner
                .spawn(move |spawner| Box::pin(process_dir(spawner, cl2, d)))
                .await;
        }
        for f in dl.files {
            let cl2 = client.clone();
            spawner.spawn(move |_spawner| process_file(cl2, f)).await;
        }
        Ok(Report::Dir)
    }
    .boxed()
}

async fn process_file(client: Client, url: Url) -> anyhow::Result<Report> {
    let target = client.get_file_redirect(url.clone()).await?;
    Ok(Report::File { url, target })
}
