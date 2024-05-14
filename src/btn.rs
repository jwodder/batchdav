use futures_util::{FutureExt, Stream};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Semaphore,
};
use tokio_util::sync::{CancellationToken, DropGuard};

type UnwindResult<T> = Result<T, Box<dyn std::any::Any + Send>>;

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
    receiver: UnboundedReceiver<UnwindResult<T>>,
    _on_drop: DropGuard,
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
        let token = CancellationToken::new();
        let (sender, receiver) = unbounded_channel();
        let spawner = Spawner {
            semaphore,
            sender,
            token: token.child_token(),
        };
        spawner.spawn_with_self(root);
        BoundedTreeNursery {
            receiver,
            _on_drop: token.drop_guard(),
        }
    }
}

impl<T: 'static> Stream for BoundedTreeNursery<T> {
    type Item = T;

    /// Poll for one of the tasks in the group to complete and return its
    /// return value.
    ///
    /// # Panics
    ///
    /// If a task panics, this method resumes unwinding the panic.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match ready!(self.receiver.poll_recv(cx)) {
            Some(Ok(r)) => Some(r).into(),
            Some(Err(e)) => std::panic::resume_unwind(e),
            None => None.into(),
        }
    }
}

/// A handle for spawning tasks in a `BoundedTreeNursery<T>`
#[derive(Debug)]
pub(crate) struct Spawner<T> {
    semaphore: Arc<Semaphore>,
    sender: UnboundedSender<UnwindResult<T>>,
    token: CancellationToken,
}

// Clone can't be derived, as that would erroneously add `T: Clone` bounds to
// the impl.
impl<T> Clone for Spawner<T> {
    fn clone(&self) -> Spawner<T> {
        Spawner {
            semaphore: self.semaphore.clone(),
            sender: self.sender.clone(),
            token: self.token.clone(),
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
        let Spawner {
            semaphore,
            sender,
            token,
        } = self.clone();
        let fut = async move {
            let Ok(_permit) = semaphore.acquire().await else {
                unreachable!("Semaphore should not be closed");
            };
            func(self).await
        };
        tokio::spawn(async move {
            tokio::select!(
                () = token.cancelled() => (),
                r = std::panic::AssertUnwindSafe(fut).catch_unwind() => {
                    let _ = sender.send(r);
                }
            );
        });
    }
}
