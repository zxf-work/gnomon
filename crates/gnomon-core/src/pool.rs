//! A persistent worker pool for the parallel read stages.
//!
//! Every parallel stage used to open a `std::thread::scope` and spawn its
//! workers per call: the select planner, the select row loop, the
//! materializer, the concatenation and the aggregate fold. On the 10M store
//! a `series.count` call paid that spawn-and-join three times, about 12 % of
//! the aggregate kernel's wall in a profile. The workers here are spawned
//! once per process and parked between calls.
//!
//! The interface is a fork-join over indices, [`run`]: `f(0)`, ..., `f(width -
//! 1)` each run exactly once, and the results come back **in index order**,
//! so a call site that used to collect its scoped handles in spawn order
//! collects the same vector. Which thread runs which index is not specified,
//! and every call site already restores its own output order (they were
//! self-scheduling over a shared claim counter before this module existed).
//!
//! The caller is a worker too: it claims indices from the same counter as the
//! pool threads. That is what makes the pool safe to use re-entrantly and
//! from many threads at once: a call never waits for an index that nobody has
//! claimed — it claims it itself — so a call made while every pool thread is
//! busy (another reader on the same handle, or a nested call from inside a
//! pool worker) still finishes, serially, rather than deadlocking.
//!
//! Lifetime erasure: `f` borrows the caller's stack. The pool threads see it
//! through a raw pointer whose lifetime has been erased, which is sound
//! because [`run`] does not return — and does not unwind — until every pool
//! thread that picked up this call has finished touching `f`. Queue entries
//! nobody has picked up yet are withdrawn by the caller before it waits, so
//! the wait is only ever for threads that are actively running.

use std::any::Any;
use std::collections::VecDeque;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

/// Upper bound on pool threads. `GNOMON_SCAN_THREADS` clamps at 64; the
/// default width is the machine's available parallelism, which may be larger
/// on a big host, so the bound is generous rather than tight.
const MAX_THREADS: usize = 512;

/// `f` with its lifetime erased; see the module comment for why it is sound.
#[derive(Clone, Copy)]
struct ErasedFn(*const (dyn Fn(usize) + Sync + 'static));
// SAFETY: the pointee is `Sync`, and `run` keeps it alive for as long as any
// thread can dereference the pointer.
unsafe impl Send for ErasedFn {}
unsafe impl Sync for ErasedFn {}

struct Job {
    f: ErasedFn,
    width: usize,
    next: AtomicUsize,
    /// Pool threads that took an entry for this job and have not yet
    /// released it. The caller waits for zero.
    running: Mutex<usize>,
    done: Condvar,
    panic: Mutex<Option<Box<dyn Any + Send>>>,
}

impl Job {
    /// Claim and run indices until none are left. Panics are caught and the
    /// first one is kept, to be resumed on the calling thread.
    fn drain(&self) {
        loop {
            let i = self.next.fetch_add(1, Ordering::Relaxed);
            if i >= self.width {
                return;
            }
            // SAFETY: see `ErasedFn`.
            let f = unsafe { &*self.f.0 };
            if let Err(p) = catch_unwind(AssertUnwindSafe(|| f(i))) {
                let mut slot = self.panic.lock().unwrap_or_else(|e| e.into_inner());
                if slot.is_none() {
                    *slot = Some(p);
                }
            }
        }
    }
}

struct Pool {
    queue: Mutex<VecDeque<Arc<Job>>>,
    ready: Condvar,
    threads: Mutex<usize>,
}

fn pool() -> &'static Pool {
    static POOL: OnceLock<Pool> = OnceLock::new();
    POOL.get_or_init(|| Pool {
        queue: Mutex::new(VecDeque::new()),
        ready: Condvar::new(),
        threads: Mutex::new(0),
    })
}

impl Pool {
    /// Grow to at least `n` threads (bounded by [`MAX_THREADS`]). Returns the
    /// thread count, which can be below `n` only if a spawn failed — in which
    /// case the caller simply does more of the work itself.
    fn ensure(&'static self, n: usize) -> usize {
        let n = n.min(MAX_THREADS);
        let mut have = self.threads.lock().unwrap_or_else(|e| e.into_inner());
        while *have < n {
            let spawned = std::thread::Builder::new()
                .name(format!("gnomon-pool-{}", *have))
                .spawn(move || self.work());
            if spawned.is_err() {
                break;
            }
            *have += 1;
        }
        *have
    }

    fn work(&self) {
        loop {
            let job = {
                let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
                loop {
                    if let Some(j) = q.pop_front() {
                        break j;
                    }
                    q = self.ready.wait(q).unwrap_or_else(|e| e.into_inner());
                }
            };
            job.drain();
            let mut running = job.running.lock().unwrap_or_else(|e| e.into_inner());
            *running -= 1;
            if *running == 0 {
                job.done.notify_all();
            }
        }
    }
}

/// Number of pool threads spawned so far (for tests).
#[cfg(test)]
pub(crate) fn threads_spawned() -> usize {
    *pool().threads.lock().unwrap_or_else(|e| e.into_inner())
}

/// Run `f(0)`, ..., `f(width - 1)`, each exactly once, on up to `width`
/// threads (the caller and `width - 1` pool threads), and return the results
/// in index order. A panic in any `f(i)` is resumed on the caller after every
/// thread has let go of `f`.
pub(crate) fn run<R, F>(width: usize, f: F) -> Vec<R>
where
    R: Send,
    F: Fn(usize) -> R + Sync,
{
    if width == 0 {
        return Vec::new();
    }
    if width == 1 {
        return vec![f(0)];
    }
    let slots: Vec<Mutex<Option<R>>> = (0..width).map(|_| Mutex::new(None)).collect();
    let g = |i: usize| {
        let r = f(i);
        *slots[i].lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
    };
    let g_ref: &(dyn Fn(usize) + Sync) = &g;
    // SAFETY: lifetime erasure only; `run` outlives every use (module comment).
    let erased: &'static (dyn Fn(usize) + Sync) = unsafe { std::mem::transmute(g_ref) };
    let job = Arc::new(Job {
        f: ErasedFn(erased as *const _),
        width,
        next: AtomicUsize::new(0),
        running: Mutex::new(0),
        done: Condvar::new(),
        panic: Mutex::new(None),
    });
    let p = pool();
    let helpers = p.ensure(width - 1).min(width - 1);
    if helpers > 0 {
        // Count the entries as running *before* they are visible: a thread
        // may pop and finish one before this function gets to the counter.
        *job.running.lock().unwrap_or_else(|e| e.into_inner()) = helpers;
        let mut q = p.queue.lock().unwrap_or_else(|e| e.into_inner());
        for _ in 0..helpers {
            q.push_back(job.clone());
        }
        drop(q);
        for _ in 0..helpers {
            p.ready.notify_one();
        }
    }
    job.drain();
    if helpers > 0 {
        // Withdraw entries no thread picked up: nobody will run them, and
        // the indices they stood for are already claimed.
        let withdrawn = {
            let mut q = p.queue.lock().unwrap_or_else(|e| e.into_inner());
            let before = q.len();
            q.retain(|j| !Arc::ptr_eq(j, &job));
            before - q.len()
        };
        let mut running = job.running.lock().unwrap_or_else(|e| e.into_inner());
        *running -= withdrawn;
        while *running > 0 {
            running = job.done.wait(running).unwrap_or_else(|e| e.into_inner());
        }
    }
    if let Some(payload) = job.panic.lock().unwrap_or_else(|e| e.into_inner()).take() {
        resume_unwind(payload);
    }
    slots
        .into_iter()
        .map(|s| {
            s.into_inner()
                .unwrap_or_else(|e| e.into_inner())
                .expect("every index runs exactly once")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn every_index_runs_once_and_results_are_in_index_order() {
        for width in [0, 1, 2, 3, 7, 16, 33] {
            let hits: Vec<AtomicU64> = (0..width).map(|_| AtomicU64::new(0)).collect();
            let got = run(width, |i| {
                hits[i].fetch_add(1, Ordering::Relaxed);
                i * 10
            });
            assert_eq!(got, (0..width).map(|i| i * 10).collect::<Vec<_>>());
            assert!(hits.iter().all(|h| h.load(Ordering::Relaxed) == 1), "width {width}");
        }
    }

    #[test]
    fn work_is_shared_with_pool_threads() {
        // Each index blocks until all four have started: only possible if four
        // threads run concurrently, i.e. the caller is not doing it all alone.
        let started = AtomicUsize::new(0);
        let got = run(4, |_| {
            started.fetch_add(1, Ordering::SeqCst);
            let t = std::time::Instant::now();
            while started.load(Ordering::SeqCst) < 4 {
                assert!(t.elapsed().as_secs() < 30, "pool threads never joined");
                std::thread::yield_now();
            }
            std::thread::current().id()
        });
        let mut ids = got.clone();
        ids.sort_by_key(|t| format!("{t:?}"));
        ids.dedup();
        assert_eq!(ids.len(), 4, "four distinct threads ran the four indices");
        assert!(threads_spawned() >= 3);
    }

    #[test]
    fn borrowed_state_is_visible_and_mutations_land() {
        let data: Vec<u64> = (0..10_000).collect();
        let next = AtomicUsize::new(0);
        let parts = run(8, |_| {
            let mut s = 0u64;
            loop {
                let k = next.fetch_add(1, Ordering::Relaxed);
                let Some(v) = data.get(k) else { break };
                s += v;
            }
            s
        });
        assert_eq!(parts.iter().sum::<u64>(), data.iter().sum::<u64>());
    }

    #[test]
    fn nested_and_concurrent_calls_finish() {
        // 8 OS threads each fan out 8 wide, and every index fans out again:
        // more demand than pool threads, and re-entrant from pool workers.
        std::thread::scope(|sc| {
            for t in 0..8u64 {
                sc.spawn(move || {
                    let got: Vec<u64> = run(8, |i| {
                        run(4, |j| t * 100 + i as u64 * 10 + j as u64).iter().sum()
                    });
                    let want: Vec<u64> = (0..8)
                        .map(|i| (0..4).map(|j| t * 100 + i * 10 + j).sum())
                        .collect();
                    assert_eq!(got, want);
                });
            }
        });
    }

    #[test]
    fn a_panic_is_resumed_on_the_caller_after_the_others_finish() {
        let finished = AtomicUsize::new(0);
        let r = catch_unwind(AssertUnwindSafe(|| {
            run(6, |i| {
                if i == 3 {
                    panic!("index three");
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
                finished.fetch_add(1, Ordering::SeqCst);
            })
        }));
        let err = r.expect_err("the panic must reach the caller");
        assert_eq!(err.downcast_ref::<&str>(), Some(&"index three"));
        assert_eq!(finished.load(Ordering::SeqCst), 5, "the other indices all ran");
        // and the pool still works afterwards
        assert_eq!(run(3, |i| i), vec![0, 1, 2]);
    }
}
