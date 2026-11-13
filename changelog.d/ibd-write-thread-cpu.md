Changed

- **Confirm parent pins no longer pay a per-thread walk.** Each pin half
  (outs, layout) was an `ArcSwap` after its first publish. In `arc-swap` a
  store and a drop each visit every thread that has used the crate, so the
  IBD write thread paid that walk for every parent pin it filled and freed.
  A half is now one `Arc` behind a mutex. A publish and a pin drop cost the
  same at any thread count, and a pin keeps one snapshot per half instead of
  the first one plus the current one.
- **The initial-download write thread finds a same-batch parent's row by
  subtraction.** The transaction ids of one write batch are a single
  contiguous run, so the commit no longer builds a hash map of the batch to
  mark outputs spent inside it. A plan whose ids are not one run could not
  commit before either. It gets the same `fk mismatch` error before its
  bodies are appended, where it used to get it after. A batch that commits
  writes the same bytes.
