[![Project Status: Concept – Minimal or no implementation has been done yet, or the repository is only intended to be a limited example, demo, or proof-of-concept.](https://www.repostatus.org/badges/latest/concept.svg)](https://www.repostatus.org/#concept)
[![CI Status](https://github.com/jwodder/batchdav/actions/workflows/test.yml/badge.svg)](https://github.com/jwodder/batchdav/actions/workflows/test.yml)
[![codecov.io](https://codecov.io/gh/jwodder/batchdav/branch/main/graph/badge.svg)](https://codecov.io/gh/jwodder/batchdav)
[![Minimum Supported Rust Version](https://img.shields.io/badge/MSRV-1.74-orange)](https://www.rust-lang.org)
[![MIT License](https://img.shields.io/github/license/jwodder/batchdav.svg)](https://opensource.org/licenses/MIT)

[GitHub](https://github.com/jwodder/batchdav) | [Issues](https://github.com/jwodder/batchdav/issues)

`batchdav` is a Rust program for traversing a WebDAV file hierarchy using a
user-specified number of concurrent worker tasks and timing how long the
traversal takes.  It was written as part of investigating dandi/dandidav#54
(primarily to double-check that the results seen there weren't due to some
quirk of rsync).

The traversal handles non-collection resources by simply making `HEAD` requests
to them without following any redirects; if the server responds with a
redirect, the original and target URL will both be printed if running `batchdav
run` without the `--quiet` option.


Usage
=====

    batchdav <command> [<args>]

`batchdav` has two subcommands: `run`, for performing a single traversal; and
`batch`, for performing multiple traversals with different numbers of workers
and summarizing the results.

Worker tasks are executed on a multithreaded asynchronous executor.  By
default, the executor uses as many threads as your machine has CPUs; a
different amount can be specified via the `TOKIO_WORKER_THREADS` environment
variable.

`run`
-----

    batchdav run [-q|--quiet] <url> <workers>

Traverse the WebDAV hierarchy at the given URL using the given number of
concurrent workers.  The elapsed time and number of requests made is printed at
the end.

If the `-q`/`--quiet` option is not given, then as each request is completed,
the URL requested is printed out along with the type of resource at that URL
(`DIR` or `FILE`) and, for non-collection resources, the URL (if any) that the
resource's URL redirects to.

`batch`
-------

    batchdav batch [-s|--samples <int>] <url> <workers> ...

Traverse the WebDAV hierarchy at the given URL repeatedly and summarize the
elapsed times.  For each number of workers listed on the command line, a
traversal is performed a number of times given by the `--samples` option
(default: 10).  Upon completion, a CSV document listing the mean & standard
deviation of the times for each number of workers is output.


Sample Results
==============

Sample `batch` output from traversing [[1][1]] on an 8-CPU 2020 Intel MacBook
Pro:

```csv
workers,time_mean,time_stddev
1,8.399036695100001,0.36142910510463516
5,1.6700318244,0.12919592271200123
10,1.0409548316000001,0.10855610294283857
15,0.7129774931999999,0.06181837739373458
20,0.750514105,0.10966455557731183
30,0.7945123642999999,0.10238084442203854
40,0.7258895968,0.08116879741778966
50,0.7132875974999999,0.07944869527032605
```

[1]: https://webdav.dandiarchive.org/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/0/0/0/0/0/
