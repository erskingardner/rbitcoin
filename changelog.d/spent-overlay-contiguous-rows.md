Changed

- **The initial-download write thread finds a same-batch parent's row by
  subtraction.** The transaction ids of one write batch are a single
  contiguous run, so the commit no longer builds a hash map of the batch to
  mark outputs spent inside it. A batch with a gap or ids out of order, as
  a retry can leave, still uses the map. The bytes written are the same.
