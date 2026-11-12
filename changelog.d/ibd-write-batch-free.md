Changed

- **IBD frees each written batch off the confirm write thread.** After a
  batch connects, the write thread hands what the batch still holds (decoded
  blocks, prepared rows, parent pins) to the existing `ibd-confirm-head`
  thread and starts the next batch. That thread frees it before the next
  batch's `tx.head` insert. Once written, a batch's wire bytes count in the
  accounted heap until it is freed. `ibd: perf` gains `thr_drop_work_ms` and
  `conf_pipe.drop_wire_bytes`. The effect on sync time has not been measured.
