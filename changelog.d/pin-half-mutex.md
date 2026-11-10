Changed

- **Confirm parent pins no longer pay a per-thread walk.** Each pin half
  (outs, layout) was an `ArcSwap` after its first publish. In `arc-swap` a
  store and a drop each visit every thread that has used the crate, so the
  IBD write thread paid that walk for every parent pin it filled and freed.
  A half is now one `Arc` behind a mutex. A publish and a pin drop cost the
  same at any thread count, and a pin keeps one snapshot per half instead of
  the first one plus the current one. Not measured on a sync yet.
