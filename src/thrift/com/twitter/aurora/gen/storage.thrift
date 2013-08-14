// Copyright 2011 Twitter, Inc.
namespace java com.twitter.aurora.gen.storage
namespace py gen.twitter.aurora.storage

include "api.thrift"

// Thrift object definitions for messages used for mesos storage.


// The valid set of keys used in the DbStorage scheduler_state kv store
enum ConfiguratonKey {

  // The current scheduler framework stored as a thrift encoded string
  FRAMEWORK_ID = 0,

  // The history of storage migration results stored as an encoded StorageMigrationResults struct
  MIGRATION_RESULTS = 1,

  // The last locally committed log position stored as a thrift encoded byte array
  LAST_COMMITTED_LOG_POSITION = 2
}

// Ops that are direct representations of the data needed to perform local storage mutations.
struct SaveFrameworkId {
  1: string id
}

struct SaveAcceptedJob {
  1: string managerId
  2: api.JobConfiguration jobConfig
}

struct SaveJobUpdate {
  5: api.JobKey jobKey
  3: string updateToken
  4: set<api.TaskUpdateConfiguration> configs
}

struct RemoveJobUpdate {
  3: api.JobKey jobKey
}

struct RemoveJob {
  2: api.JobKey jobKey
}

struct SaveTasks {
  1: set<api.ScheduledTask> tasks
}

struct RewriteTask {
  1: string taskId
  2: api.TaskConfig task
}

struct RemoveTasks {
  1: set<string> taskIds
}

struct SaveQuota {
  1: string role
  2: api.Quota quota
}

struct RemoveQuota {
  1: string role
}

struct SaveHostAttributes {
  1: api.HostAttributes hostAttributes
}

union Op {
  1: SaveFrameworkId saveFrameworkId
  2: SaveAcceptedJob saveAcceptedJob
  3: SaveJobUpdate saveJobUpdate
  4: RemoveJobUpdate removeJobUpdate
  5: RemoveJob removeJob
  6: SaveTasks saveTasks
  7: RemoveTasks removeTasks
  8: SaveQuota saveQuota
  9: RemoveQuota removeQuota
  10: SaveHostAttributes saveHostAttributes
  11: RewriteTask rewriteTask
}

// Represents a series of local storage mutations that should be applied in a single atomic
// transaction.
struct Transaction {
  1: list<Op> ops
}

struct StoredJob {
  1: string jobManagerId
  3: api.JobConfiguration jobConfiguration
}

struct SchedulerMetadata {
  1: string frameworkId
}

struct QuotaConfiguration {
  1: string role
  2: api.Quota quota
}

// Represents a complete snapshot of local storage data suitable for restoring the local storage
// system to its state at the time the snapshot was taken.
struct Snapshot {

  // The timestamp when the snapshot was made in milliseconds since the epoch.
  1: i64 timestamp

  3: set<api.HostAttributes> hostAttributes
  4: set<api.ScheduledTask> tasks
  5: set<StoredJob> jobs
  6: SchedulerMetadata schedulerMetadata
  7: set<api.JobUpdateConfiguration> updateConfigurations
  8: set<QuotaConfiguration> quotaConfigurations
}

// A message header that calls out the number of expected FrameChunks to follow to form a complete
// message.
struct FrameHeader {

  // The number of FrameChunks following this FrameHeader required to reconstitute its message.
  1: i32 chunkCount

  // The MD5 checksum over the binary blob that was chunked across chunkCount chunks to decompose
  // the message.
  2: binary checksum
}

// A chunk of binary data that can be assembled with others to reconstitute a fully framed message.
struct FrameChunk {
  2: binary data
}

// Frames form a series of LogEntries that can be re-assembled into a basic log entry type like a
// Snapshot.  The Frame protocol is that a single FrameHeader is followed by one or more FrameChunks
// that can be re-assembled to obtain the binary content of a basic log entry type.
//
// In the process of reading a Frame, invalid data should always be logged and skipped as it may
// represent a failed higher level transaction where a FrameHeader successfully appends but not all
// the chunks required to complete the full message frame successfully commit.  For example: if a
// Snaphsot is framed, it might break down into 1 FrameHeader followed by 5 FrameChunks.  It could
// be that the FrameHeader and 2 chunks get written successfully, but the 3rd and subsequent chunks
// fail to append.  In this case, the storage mechanism would throw to indicate a failed transaction
// at write-time leaving a partially framed message in the log stream that should be skipped over at
// read-time.
union Frame {
  1: FrameHeader header
  2: FrameChunk chunk
}

// A scheduler storage write-ahead log entry consisting of no-ops to skip over or else snapshots or
// transactions to apply.  Any entry type can also be chopped up into frames if the entry is too big
// for whatever reason.
union LogEntry {
  1: Snapshot snapshot
  2: Transaction transaction

  // The value should be ignored - both true and false signal an equivalent no operation marker.
  3: bool noop;

  4: Frame frame

  // A LogEntry that is first serialized in the thrift binary format,
  // then compressed using the "deflate" compression format.
  // Deflated entries are expected to be un-framed.  They may be pieced together by multiple frames,
  // but the contents of the deflated entry should not be a Frame.
  5: binary deflatedEntry
}

