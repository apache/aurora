# Snapshot Performance

Periodically the scheduler writes a full snapshot of its state to the replicated log. To do this
it needs to hold a global storage write lock while it writes out this data. In large clusters
this has been observed to take up to 40 seconds. Long pauses can cause issues in the system,
including delays in scheduling new tasks.

The scheduler has two optimizations to reduce the size of snapshots and thus improve snapshot
performance: compression and deduplication. Most users will want to enable both compression
and deduplication.

## Compression

To reduce the size of the snapshot the DEFLATE algorithm can be applied to the serialized bytes
of the snapshot as they are written to the stream. This reduces the total number of bytes that
need to be written to the replicated log at the cost of CPU and generally reduces the amount
of time a snapshot takes. Most users will want to enable both compression and deduplication.

### Enabling Compression

Snapshot compression is enabled via the `-deflate_snapshots` flag. This is the default since
Aurora 0.5.0. All released versions of Aurora can read both compressed and uncompressed snapshots,
so there are no backwards compatibility concerns associated with changing this flag.

### Disabling compression

Disable compression by passing `-deflate_snapshots=false`.

## Deduplication

In Aurora 0.6.0 a new snapshot format was introduced. Rather than write one configuration blob
per Mesos task this format stores each configuration blob once, and each Mesos task with a
pointer to its blob. This format is not backwards compatible with earlier versions of Aurora.

### Enabling Deduplication

After upgrading Aurora to 0.6.0, enable deduplication with the `-deduplicate_snapshots` flag.
After the first snapshot the cluster will be using the deduplicated format to write to the
replicated log. Snapshots are created periodically by the scheduler (according to
the `-dlog_snapshot_interval` flag). An administrator can also force a snapshot operation with
`aurora_admin snapshot`.

### Disabling Deduplication

To disable deduplication, for example to rollback to Aurora, restart all of the cluster's
schedulers with `-deduplicate_snapshots=false` and either wait for a snapshot or force one
using `aurora_admin snapshot`.
