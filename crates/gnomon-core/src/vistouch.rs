//! How much transaction-time and close state a query's belief test
//! touched.
//!
//! The engine's cost claim for visibility is a claim about *what is read*.
//! The claim is "the runs, codes and closes inside the query's row range,
//! not the segment's". Timers cannot pin that claim: a 6.5M-code walk and
//! a 16-code walk differ by milliseconds on one host and by less on a
//! faster one. These counters count the items instead. Each visibility
//! primitive adds what it walked, **once per call** (never per item), at
//! the point where it walks it, so a test reads the production path's own
//! numbers rather than a re-derivation of them.
//!
//! Four quantities:
//!
//! * `codes`: dictionary-coded `tt_s` codes read row by row (rows in a
//!   block whose bounds straddle the belief cut);
//! * `runs`: `tt_s` run-list entries walked;
//! * `closes`: `(row, tt_e)` close entries copied, merged or walked;
//! * `steps`: inner iterations of the range-list set difference
//!   (`beliefdiff::range_subtract`), the one primitive whose cost is not a
//!   count of stored items.
//!
//! Observation only. Nothing in the engine reads these back. They are
//! compiled out with the timers (`--no-default-features`), exactly like
//! `ingest_phase`. Two sinks: a **thread-local** accumulator, which the
//! Rust tests read (a test thread sees only its own work, so parallel tests
//! cannot pollute each other), and the process-global [`crate::phase`]
//! slots `vis_*`, which `phase_stats()` exposes to a Python driver.

/// Items a belief test touched. See the module note for each field.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VisTouch {
    pub codes: u64,
    pub runs: u64,
    pub closes: u64,
    pub steps: u64,
}

impl VisTouch {
    /// Stored items read: codes + runs + closes. `steps` is excluded
    /// because it counts work over derived range lists, not stored items.
    pub fn items(&self) -> u64 {
        self.codes + self.runs + self.closes
    }
}

/// Are the counters compiled in?
pub const ENABLED: bool = cfg!(feature = "ingest-timers");

#[cfg(feature = "ingest-timers")]
thread_local! {
    static TOUCH: std::cell::Cell<VisTouch> = const {
        std::cell::Cell::new(VisTouch { codes: 0, runs: 0, closes: 0, steps: 0 })
    };
}

#[cfg(feature = "ingest-timers")]
#[inline]
fn bump(f: impl FnOnce(&mut VisTouch), p: crate::phase::Phase, n: u64) {
    TOUCH.with(|c| {
        let mut t = c.get();
        f(&mut t);
        c.set(t);
    });
    crate::phase::add(p, n);
}

/// `n` dictionary codes read.
#[inline]
pub(crate) fn codes(n: usize) {
    #[cfg(feature = "ingest-timers")]
    if n > 0 {
        bump(|t| t.codes += n as u64, crate::phase::Phase::VisCodes, n as u64);
    }
    #[cfg(not(feature = "ingest-timers"))]
    let _ = n;
}

/// `n` run-list entries walked.
#[inline]
pub(crate) fn runs(n: usize) {
    #[cfg(feature = "ingest-timers")]
    if n > 0 {
        bump(|t| t.runs += n as u64, crate::phase::Phase::VisRuns, n as u64);
    }
    #[cfg(not(feature = "ingest-timers"))]
    let _ = n;
}

/// `n` close entries copied, merged or walked.
#[inline]
pub(crate) fn closes(n: usize) {
    #[cfg(feature = "ingest-timers")]
    if n > 0 {
        bump(|t| t.closes += n as u64, crate::phase::Phase::VisCloses, n as u64);
    }
    #[cfg(not(feature = "ingest-timers"))]
    let _ = n;
}

/// `n` set-difference inner iterations.
#[inline]
pub(crate) fn steps(n: usize) {
    #[cfg(feature = "ingest-timers")]
    if n > 0 {
        bump(|t| t.steps += n as u64, crate::phase::Phase::VisSteps, n as u64);
    }
    #[cfg(not(feature = "ingest-timers"))]
    let _ = n;
}

/// This thread's counts since the last `take`, and reset them. Always
/// zero when the counters are compiled out.
pub fn take() -> VisTouch {
    #[cfg(feature = "ingest-timers")]
    {
        TOUCH.with(|c| c.replace(VisTouch::default()))
    }
    #[cfg(not(feature = "ingest-timers"))]
    {
        VisTouch::default()
    }
}
