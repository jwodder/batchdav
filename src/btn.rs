use futures_util::Stream;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinSet,
};

#[derive(Clone, Debug)]
pub(crate) struct BoundedTreeNursery<T> {
    tasks: Arc<Mutex<JoinSet<T>>>,
}

impl<T: Send + 'static> BoundedTreeNursery<T> {
    pub(crate) async fn new<F, Fut>(limit: usize, top: F) -> Self
    where
        F: FnOnce(Spawner<T>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let semaphore = Arc::new(Semaphore::new(limit));
        let tasks = Arc::new(Mutex::new(JoinSet::new()));
        let spawner = Spawner {
            semaphore,
            tasks: tasks.clone(),
        };
        spawner.spawn_with_self(top).await;
        BoundedTreeNursery { tasks }
    }
}

impl<T: 'static> Stream for BoundedTreeNursery<T> {
    type Item = T;

    /// # Panics
    ///
    /// If a task panics, this method resumes unwinding the panic.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let Ok(mut tasks) = self.tasks.try_lock() else {
            return Poll::Pending;
        };
        loop {
            match ready!(tasks.poll_join_next(cx)) {
                Some(Ok(r)) => return Some(r).into(),
                Some(Err(e)) => {
                    if let Ok(barf) = e.try_into_panic() {
                        std::panic::resume_unwind(barf);
                    }
                    // else: task was aborted; loop around & poll again
                }
                None => {
                    if Arc::strong_count(&self.tasks) == 1 {
                        // All spawners dropped and all results yielded; end of
                        // stream
                        return None.into();
                    } else {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct Spawner<T> {
    semaphore: Arc<Semaphore>,
    tasks: Arc<Mutex<JoinSet<T>>>,
}

// Clone can't be derived, as that would erroneously add `T: Clone` bounds to
// the impl.
impl<T> Clone for Spawner<T> {
    fn clone(&self) -> Spawner<T> {
        Spawner {
            semaphore: self.semaphore.clone(),
            tasks: self.tasks.clone(),
        }
    }
}

impl<T: Send + 'static> Spawner<T> {
    pub(crate) async fn spawn<F, Fut>(&self, func: F)
    where
        F: FnOnce(Spawner<T>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.clone().spawn_with_self(func).await;
    }

    async fn spawn_with_self<F, Fut>(self, func: F)
    where
        F: FnOnce(Spawner<T>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let semaphore = self.semaphore.clone();
        self.tasks.clone().lock().await.spawn(async move {
            let Ok(_permit) = semaphore.acquire().await else {
                unreachable!("Semaphore should not be closed");
            };
            func(self).await
        });
    }
}