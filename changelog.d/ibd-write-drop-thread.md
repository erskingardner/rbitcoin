Changed

- **IBD frees each written batch off the confirm write thread.** After a
  batch connects, the write thread hands what the batch still holds (decoded
  blocks, prepared rows, parent pins) to a new `ibd-confirm-drop` thread and
  starts the next batch. One batch may wait and one is being freed. When that
  queue is full the write thread frees the batch itself. A waiting batch's
  wire bytes stay in the accounted heap until it is freed. `ibd: perf` gains
  `thr_drop_work_ms`, `write_drop_inline`, and `conf_pipe.drop_wire_bytes`.
  The effect on sync time has not been measured.
