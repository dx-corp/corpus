-- Replay ledger for Merlin segment batches. Corpus stores only the
-- observations that name an artifact digest; every other event stays in
-- the endpoint spool. A replayed batch is recognized by its digest, so
-- its receipt and the segment's event_count stay exact without a row per
-- raw event. One row per batch, not per event.
CREATE TABLE IF NOT EXISTS merlin_segment_batch (
  segment_id       uuid NOT NULL REFERENCES merlin_segment (id),
  batch_sha256     text NOT NULL CHECK (batch_sha256 ~ '^[a-f0-9]{64}$'),
  event_count      integer NOT NULL CHECK (event_count >= 0),
  accepted_events  integer NOT NULL CHECK (accepted_events >= 0 AND accepted_events <= event_count),
  received_at      timestamptz NOT NULL,
  PRIMARY KEY (segment_id, batch_sha256)
);
