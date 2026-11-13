// Bench only, not part of the node. Times the lookup thread's
// undrained-creates set by itself: the two txid maps on
// query/lookup-undrained-creates against a fixed table of 8-byte slots
// (40-bit keyed fingerprint + 24-bit level), eight slots to a 64-byte bucket.
//
// Build: rustc -O -C target-cpu=native setbench.rs
// Run:   ./setbench [txids per block] [% of probes that name a live create] [cold 0|1]
//
// Each block: note its txids, probe two prev txids per txid, probe the held
// ones a second time (the wave's readmit). Time is this thread's CPU time.
// With cold=1 a 64 MiB buffer is rewritten between blocks, outside the timed
// part, so the set starts each block out of cache.
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};

#[repr(C)]
struct Timespec {
    sec: i64,
    nsec: i64,
}
extern "C" {
    fn clock_gettime(clk: i32, ts: *mut Timespec) -> i32;
}
/// Nanoseconds of CPU this thread has used (CLOCK_THREAD_CPUTIME_ID).
fn cpu_ns() -> u128 {
    let mut ts = Timespec { sec: 0, nsec: 0 };
    unsafe { clock_gettime(3, &mut ts) };
    ts.sec as u128 * 1_000_000_000 + ts.nsec as u128
}

#[derive(Default, Clone, Copy)]
struct TxidHasher(u64);
impl Hasher for TxidHasher {
    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() >= 8 {
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&bytes[..8]);
            self.0 = u64::from_le_bytes(raw);
        }
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn txid(&mut self) -> [u8; 32] {
        let mut t = [0u8; 32];
        for c in t.chunks_mut(8) {
            c.copy_from_slice(&self.next().to_le_bytes());
        }
        t
    }
}

trait Set {
    fn note_block(&mut self, h: u32, txids: &[[u8; 32]]);
    fn drop_through(&mut self, hi: u32);
    fn holds(&self, t: &[u8; 32]) -> bool;
    fn holds_many(&self, probes: &[[u8; 32]], out: &mut [bool]) {
        for (o, p) in out.iter_mut().zip(probes) {
            *o = self.holds(p);
        }
    }
    fn bytes(&self) -> usize;
}

// The branch today: whole txids with their heights in maps that only fill
// and are freed whole.
struct Segments {
    seg: usize,
    max: usize,
    segs: VecDeque<(u32, HashMap<[u8; 32], u32, BuildHasherDefault<TxidHasher>>)>,
    floor: u32,
}
impl Segments {
    fn new(seg: usize, max: usize) -> Self {
        Segments { seg, max, segs: VecDeque::new(), floor: 0 }
    }
}
impl Set for Segments {
    fn note_block(&mut self, h: u32, txids: &[[u8; 32]]) {
        let fits = self.segs.back().is_some_and(|s| s.1.len() + txids.len() <= self.seg);
        if !fits {
            self.segs.push_back((h, HashMap::default()));
            if self.segs.len() > self.max {
                self.segs.pop_front();
            }
        }
        let back = self.segs.back_mut().unwrap();
        for t in txids {
            back.1.insert(*t, h);
        }
        back.0 = h;
    }
    fn drop_through(&mut self, hi: u32) {
        self.floor = self.floor.max(hi);
        while self.segs.front().is_some_and(|s| s.0 <= self.floor) {
            self.segs.pop_front();
        }
    }
    fn holds(&self, t: &[u8; 32]) -> bool {
        for s in self.segs.iter().rev() {
            if let Some(&h) = s.1.get(t) {
                return h > self.floor;
            }
        }
        false
    }
    fn bytes(&self) -> usize {
        self.segs
            .iter()
            .map(|s| match s.1.capacity() {
                0 => 0,
                c => (c * 8 / 7).next_power_of_two() * 37,
            })
            .sum()
    }
}

#[inline(always)]
fn words(t: &[u8; 32]) -> [u64; 4] {
    let mut w = [0u64; 4];
    for (i, c) in t.chunks_exact(8).enumerate() {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(c);
        w[i] = u64::from_le_bytes(raw);
    }
    w
}

/// SipHash-c-d of exactly 32 bytes.
#[inline(always)]
fn sip(k0: u64, k1: u64, m: [u64; 4], c: u32, d: u32) -> u64 {
    let mut v0 = k0 ^ 0x736f_6d65_7073_6575;
    let mut v1 = k1 ^ 0x646f_7261_6e64_6f6d;
    let mut v2 = k0 ^ 0x6c79_6765_6e65_7261;
    let mut v3 = k1 ^ 0x7465_6462_7974_6573;
    macro_rules! round {
        () => {
            v0 = v0.wrapping_add(v1);
            v1 = v1.rotate_left(13);
            v1 ^= v0;
            v0 = v0.rotate_left(32);
            v2 = v2.wrapping_add(v3);
            v3 = v3.rotate_left(16);
            v3 ^= v2;
            v0 = v0.wrapping_add(v3);
            v3 = v3.rotate_left(21);
            v3 ^= v0;
            v2 = v2.wrapping_add(v1);
            v1 = v1.rotate_left(17);
            v1 ^= v2;
            v2 = v2.rotate_left(32);
        };
    }
    for w in m.iter().copied().chain(std::iter::once(32u64 << 56)) {
        v3 ^= w;
        for _ in 0..c {
            round!();
        }
        v0 ^= w;
    }
    v2 ^= 0xff;
    for _ in 0..d {
        round!();
    }
    v0 ^ v1 ^ v2 ^ v3
}

#[derive(Clone, Copy, PartialEq)]
enum Mix {
    /// First eight bytes of the txid, no key. The floor, not a candidate.
    Ident,
    /// SipHash-1-3 written out for a 32-byte key.
    Sip13,
    /// SipHash-2-4 written out for a 32-byte key.
    Sip24,
}

const LEVEL_BITS: u32 = 24;
const LEVEL_MASK: u64 = (1 << LEVEL_BITS) - 1;

// Reviewer's table: 2^log2 buckets of eight 8-byte slots. A slot is a
// 40-bit fingerprint over a 24-bit level; level 0 is empty.
struct Bucket8 {
    /// Hash a whole block's keys first, then touch the table.
    batched: bool,
    mix: Mix,
    k0: u64,
    k1: u64,
    slots: Vec<[u64; 8]>,
    shift: u32,
    dead_through: u64,
    evicted_live: u64,
}
impl Bucket8 {
    fn new(log2_buckets: u32, mix: Mix, seed: u64, batched: bool) -> Self {
        let mut r = Rng(seed);
        Bucket8 {
            batched,
            mix,
            k0: r.next(),
            k1: r.next(),
            slots: vec![[0u64; 8]; 1 << log2_buckets],
            shift: 64 - log2_buckets,
            dead_through: 0,
            evicted_live: 0,
        }
    }
    #[inline(always)]
    fn hash(&self, t: &[u8; 32]) -> u64 {
        match self.mix {
            Mix::Ident => words(t)[0],
            Mix::Sip13 => sip(self.k0, self.k1, words(t), 1, 3),
            Mix::Sip24 => sip(self.k0, self.k1, words(t), 2, 4),
        }
    }
}
impl Bucket8 {
    #[inline(always)]
    fn probe(&self, x: u64) -> bool {
        let fp = (x & ((1 << 40) - 1)) << LEVEL_BITS;
        let b = &self.slots[(x >> self.shift) as usize];
        let mut hit = false;
        for i in 0..8 {
            let s = b[i];
            hit |= s & !LEVEL_MASK == fp && s & LEVEL_MASK > self.dead_through;
        }
        hit
    }
}
impl Set for Bucket8 {
    fn holds_many(&self, probes: &[[u8; 32]], out: &mut [bool]) {
        if !self.batched {
            for (o, p) in out.iter_mut().zip(probes) {
                *o = self.holds(p);
            }
            return;
        }
        let hashes: Vec<u64> = probes.iter().map(|p| self.hash(p)).collect();
        for (o, x) in out.iter_mut().zip(hashes) {
            *o = self.probe(x);
        }
    }
    fn note_block(&mut self, h: u32, txids: &[[u8; 32]]) {
        let level = (h as u64 + 1) & LEVEL_MASK;
        let hashes: Vec<u64> = if self.batched {
            txids.iter().map(|t| self.hash(t)).collect()
        } else {
            Vec::new()
        };
        for (n, t) in txids.iter().enumerate() {
            let x = if self.batched { hashes[n] } else { self.hash(t) };
            let fp = (x & ((1 << 40) - 1)) << LEVEL_BITS;
            let b = &mut self.slots[(x >> self.shift) as usize];
            let mut victim = 0usize;
            let mut low = u64::MAX;
            let mut done = false;
            for i in 0..8 {
                let s = b[i];
                let lv = s & LEVEL_MASK;
                if s & !LEVEL_MASK == fp && lv != 0 {
                    b[i] = fp | level;
                    done = true;
                    break;
                }
                if lv < low {
                    low = lv;
                    victim = i;
                }
            }
            if !done {
                if low > self.dead_through {
                    self.evicted_live += 1;
                }
                b[victim] = fp | level;
            }
        }
    }
    fn drop_through(&mut self, hi: u32) {
        self.dead_through = self.dead_through.max(hi as u64 + 1);
    }
    fn holds(&self, t: &[u8; 32]) -> bool {
        let x = self.hash(t);
        let fp = (x & ((1 << 40) - 1)) << LEVEL_BITS;
        let b = &self.slots[(x >> self.shift) as usize];
        let mut hit = false;
        for i in 0..8 {
            let s = b[i];
            hit |= s & !LEVEL_MASK == fp && s & LEVEL_MASK > self.dead_through;
        }
        hit
    }
    fn bytes(&self) -> usize {
        self.slots.len() * 64
    }
}

#[derive(Clone, Copy)]
enum Ages {
    /// A pipeline parent is equally likely to be any live block.
    Uniform,
    /// Its age in blocks is geometric with this mean.
    Geometric(f64),
}

struct Out {
    ms: f64,
    peak: usize,
    live_probes: u64,
    live_misses: u64,
    false_hits: u64,
}

struct Shape {
    tx: usize,
    live_pct: u64,
    cold: bool,
}

fn run(set: &mut dyn Set, shape: &Shape, live_blocks: u32, timed: u32, ages: Ages) -> Out {
    const WAVE: u32 = 8;
    let (tx, probes_n) = (shape.tx, shape.tx * 2);
    let mut rng = Rng(42);
    let warm = live_blocks + 64;
    let mut recent: VecDeque<(u32, Vec<[u8; 32]>)> = VecDeque::new();
    let mut dead = 0u32;
    let mut out = Out { ms: 0.0, peak: 0, live_probes: 0, live_misses: 0, false_hits: 0 };
    let mut ns = 0u128;
    let mut held: Vec<bool> = vec![false; probes_n];
    let mut again: Vec<bool> = Vec::with_capacity(probes_n);
    let mut evict: Vec<u8> = if shape.cold { vec![0u8; 64 << 20] } else { Vec::new() };
    for h in 1..=(warm + timed) {
        let txids: Vec<[u8; 32]> = (0..tx).map(|_| rng.txid()).collect();
        let mut probes: Vec<[u8; 32]> = Vec::with_capacity(probes_n);
        let mut is_live: Vec<bool> = Vec::with_capacity(probes_n);
        let live_n = recent.iter().filter(|(bh, _)| *bh > dead).count();
        for _ in 0..probes_n {
            if rng.next() % 100 < shape.live_pct && live_n > 0 {
                let age = match ages {
                    Ages::Uniform => (rng.next() as usize) % live_n,
                    Ages::Geometric(mean) => loop {
                        let u = (rng.next() >> 11) as f64 / (1u64 << 53) as f64;
                        let a = (u.max(1e-18).ln() / (1.0 - 1.0 / mean).ln()) as usize;
                        if a < live_n {
                            break a;
                        }
                    },
                };
                let b = &recent[recent.len() - 1 - age].1;
                probes.push(b[(rng.next() as usize) % b.len()]);
                is_live.push(true);
            } else {
                probes.push(rng.txid());
                is_live.push(false);
            }
        }
        if shape.cold {
            let fill = h as u8;
            for b in evict.iter_mut().step_by(64) {
                *b = fill;
            }
        }
        let t = cpu_ns();
        set.note_block(h, &txids);
        set.holds_many(&probes, &mut held);
        if h % WAVE == 0 {
            dead = h.saturating_sub(live_blocks);
            set.drop_through(dead);
        }
        // The wave's readmit: every prev that was held is probed again.
        let skipped: Vec<[u8; 32]> =
            probes.iter().zip(&held).filter(|(_, h)| **h).map(|(p, _)| *p).collect();
        again.clear();
        again.resize(skipped.len(), false);
        set.holds_many(&skipped, &mut again);
        let dt = cpu_ns() - t;
        if h > warm {
            ns += dt;
            out.peak = out.peak.max(set.bytes());
            for (i, live) in is_live.iter().enumerate() {
                if *live {
                    out.live_probes += 1;
                    out.live_misses += !held[i] as u64;
                } else {
                    out.false_hits += held[i] as u64;
                }
            }
        }
        recent.push_back((h, txids));
        if recent.len() as u32 > live_blocks {
            recent.pop_front();
        }
    }
    out.ms = ns as f64 / timed as f64 / 1e6;
    out
}

fn main() {
    // Self-check of the written-out SipHash against std (keys 0,0).
    {
        let mut r = Rng(7);
        for _ in 0..1000 {
            let t = r.txid();
            #[allow(deprecated)]
            let mut s = std::hash::SipHasher::new_with_keys(0, 0);
            s.write(&t);
            assert_eq!(s.finish(), sip(0, 0, words(&t), 2, 4), "SipHash-2-4 mismatch");
        }
    }
    let args: Vec<String> = std::env::args().collect();
    let shape = Shape {
        tx: args.get(1).and_then(|s| s.parse().ok()).unwrap_or(3600),
        live_pct: args.get(2).and_then(|s| s.parse().ok()).unwrap_or(43),
        cold: args.get(3).is_some_and(|s| s == "1"),
    };
    let timed = 600;
    println!(
        "{} creates and {} probes per block, {}% of probes name a live create, cold={}; thread CPU time, best of 7",
        shape.tx,
        shape.tx * 2,
        shape.live_pct,
        shape.cold as u8
    );
    println!(
        "{:<44} {:>9} {:>5} {:>8} {:>6} {:>10} {:>10}",
        "layout", "live txid", "ages", "ms/block", "MiB", "live miss%", "false hits"
    );
    for live_txids in [50_000usize, 100_000, 200_000, 300_000, 450_000] {
        let live_blocks = (live_txids / shape.tx) as u32;
        for (aname, ages) in [("unif", Ages::Uniform), ("geo8", Ages::Geometric(8.0))] {
            let row = |name: &str, mk: &dyn Fn() -> Box<dyn Set>| {
                let mut best: Option<Out> = None;
                for _ in 0..7 {
                    let mut s = mk();
                    let o = run(s.as_mut(), &shape, live_blocks, timed, ages);
                    if best.as_ref().is_none_or(|b| o.ms < b.ms) {
                        best = Some(o);
                    }
                }
                let o = best.unwrap();
                println!(
                    "{:<44} {:>9} {:>5} {:>8.3} {:>6.1} {:>10.3} {:>10}",
                    name,
                    live_txids,
                    aname,
                    o.ms,
                    o.peak as f64 / (1 << 20) as f64,
                    100.0 * o.live_misses as f64 / o.live_probes.max(1) as f64,
                    o.false_hits
                );
            };
            row("two maps of 450,000 (branch)", &|| Box::new(Segments::new(450_000, 2)));
            row("two maps of 225,000", &|| Box::new(Segments::new(225_000, 2)));
            row("fingerprints, no key (floor)", &|| Box::new(Bucket8::new(17, Mix::Ident, 1, false)));
            row("fingerprints, SipHash-1-3", &|| Box::new(Bucket8::new(17, Mix::Sip13, 1, false)));
            row("fingerprints, SipHash-1-3, hash then probe", &|| Box::new(Bucket8::new(17, Mix::Sip13, 1, true)));
            row("fingerprints, SipHash-2-4, hash then probe", &|| Box::new(Bucket8::new(17, Mix::Sip24, 1, true)));
        }
    }
}
