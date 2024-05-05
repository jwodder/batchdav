use futures_util::Stream;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Semaphore,
    },
    task::{JoinError, JoinHandle, JoinSet},
};

/// A task group with the following properties:
///
/// - No more than a certain number of tasks are ever active at once.
///
/// - Each task is passed a `Spawner` that can be used to spawn more tasks in
///   the group.
///
/// - `BoundedTreeNursery<T>` is a `Stream` of the return values of the tasks
///   (which must all be `T`).
///
/// - Dropping `BoundedTreeNursery` causes all tasks to be aborted.
#[derive(Debug)]
pub(crate) struct BoundedTreeNursery<T> {
    receiver: UnboundedReceiver<FragileHandle<T>>,
    tasks: JoinSet<Result<T, JoinError>>,
    closed: bool,
}

impl<T: Send + 'static> BoundedTreeNursery<T> {
    /// Create a `BoundedTreeNursery` that limits the number of active tasks to
    /// at most `limit` and with `root` spawned as the initial task
    pub(crate) fn new<F, Fut>(limit: usize, root: F) -> Self
    where
        F: FnOnce(Spawner<T>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let semaphore = Arc::new(Semaphore::new(limit));
        let (sender, receiver) = unbounded_channel();
        let spawner = Spawner { semaphore, sender };
        spawner.spawn_with_self(root);
        BoundedTreeNursery {
            tasks: JoinSet::new(),
            receiver,
            closed: false,
        }
    }
}

impl<T: Send + 'static> Stream for BoundedTreeNursery<T> {
    type Item = T;

    /// Poll for one of the tasks in the group to complete and return its
    /// return value.
    ///
    /// # Panics
    ///
    /// If a task panics, this method resumes unwinding the panic.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut buf = Vec::new();
        match self.receiver.poll_recv_many(cx, &mut buf, 32) {
            Poll::Pending => (),
            Poll::Ready(0) => self.closed = true,
            Poll::Ready(_) => {
                for handle in buf {
                    self.tasks.spawn(handle);
                }
            }
        }
        match ready!(self.tasks.poll_join_next(cx)) {
            Some(Ok(Ok(r))) => Some(r).into(),
            Some(Ok(Err(e)) | Err(e)) => match e.try_into_panic() {
                Ok(barf) => std::panic::resume_unwind(barf),
                Err(e) => unreachable!(
                    "Task in BoundedTreeNursery should not have been aborted, but got {e:?}"
                ),
            },
            None => {
                if self.closed {
                    // All spawners dropped and all results yielded; end of
                    // stream
                    None.into()
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

/// A handle for spawning tasks in a `BoundedTreeNursery<T>`
#[derive(Debug)]
pub(crate) struct Spawner<T> {
    semaphore: Arc<Semaphore>,
    sender: UnboundedSender<FragileHandle<T>>,
}

// Clone can't be derived, as that would erroneously add `T: Clone` bounds to
// the impl.
impl<T> Clone for Spawner<T> {
    fn clone(&self) -> Spawner<T> {
        Spawner {
            semaphore: self.semaphore.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<T: Send + 'static> Spawner<T> {
    /// Spawn the given task in the task group, passing it a new `Spawner`
    pub(crate) fn spawn<F, Fut>(&self, func: F)
    where
        F: FnOnce(Spawner<T>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.clone().spawn_with_self(func);
    }

    /// Spawn the given task in the task group, passing it this `Spawner`
    fn spawn_with_self<F, Fut>(self, func: F)
    where
        F: FnOnce(Spawner<T>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let semaphore = self.semaphore.clone();
        let sender = self.sender.clone();
        let _ = sender.send(FragileHandle::new(tokio::spawn(async move {
            let Ok(_permit) = semaphore.acquire().await else {
                unreachable!("Semaphore should not be closed");
            };
            func(self).await
        })));
    }
}

pin_project! {
    /// A wrapper around `tokio::task::JoinHandle` that aborts the task on drop.
    #[derive(Debug)]
    struct FragileHandle<T> {
        #[pin]
        inner: JoinHandle<T>
    }

    impl<T> PinnedDrop for FragileHandle<T> {
        fn drop(this: Pin<&mut Self>) {
            this.project().inner.abort();
        }
    }
}

impl<T> FragileHandle<T> {
    fn new(inner: JoinHandle<T>) -> Self {
        FragileHandle { inner }
    }
}

impl<T> Future for FragileHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.inner.poll(cx)
    }
}
