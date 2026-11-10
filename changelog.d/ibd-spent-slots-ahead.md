Changed

- **IBD reads spent slots while Class A appends.** The confirm write stage
  read the spent slots of each block's parents on the write thread, one block
  at a time, after the Class A append. It now reads a write batch's slots
  once. Parents archived by an earlier write are read on a new process-wide
  `ibd-confirm-spent` thread while the write thread appends, and joined before
  structural. That covers parents the previous write archived while this
  batch was already loaded. Slots of creates the batch itself appended are read after the
  append, as before. `--datadir-hdd` reads nothing ahead of the append. Both
  double-spend rejections are unchanged. `ibd: perf` gains `meta_reads`,
  `meta_ahead_n`, and `write.slots_join_ms`; `meta_ms` now counts the read on
  either thread, and the part read ahead is no longer inside
  `write.structural_ms`. No speed-up has been measured yet.
