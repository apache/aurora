/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace java org.apache.aurora.gen.storage
namespace py gen.apache.aurora.storage

include "api.thrift"

// Thrift object definitions for messages used for mesos storage.

// Ops that are direct representations of the data needed to perform local storage mutations.
struct SaveFrameworkId {
  1: string id
}

struct SaveCronJob {
  2: api.JobConfiguration jobConfig
}

struct RemoveJob {
  2: api.JobKey jobKey
}

struct SaveTasks {
  1: set<api.ScheduledTask> tasks
}

struct RemoveTasks {
  1: set<string> taskIds
}

struct SaveQuota {
  1: string role
  2: api.ResourceAggregate quota
}

struct RemoveQuota {
  1: string role
}

struct SaveHostAttributes {
  1: api.HostAttributes hostAttributes
}

struct SaveJobUpdate {
  1: api.JobUpdate jobUpdate
  // 2: deleted
}

struct StoredJobUpdateDetails {
  1: api.JobUpdateDetails details
  // 2: deleted
}

struct SaveJobUpdateEvent {
  1: api.JobUpdateEvent event
  3: api.JobUpdateKey key
}

struct SaveJobInstanceUpdateEvent {
  1: api.JobInstanceUpdateEvent event
  3: api.JobUpdateKey key
}

struct PruneJobUpdateHistory {
  1: i32 perJobRetainCount
  2: i64 historyPruneThresholdMs
}

union Op {
  1: SaveFrameworkId saveFrameworkId
  2: SaveCronJob saveCronJob
  5: RemoveJob removeJob
  6: SaveTasks saveTasks
  7: RemoveTasks removeTasks
  8: SaveQuota saveQuota
  9: RemoveQuota removeQuota
  10: SaveHostAttributes saveHostAttributes
  // 11: removed
  // 12: deleted
  // 13: deleted
  14: SaveJobUpdate saveJobUpdate
  15: SaveJobUpdateEvent saveJobUpdateEvent
  16: SaveJobInstanceUpdateEvent saveJobInstanceUpdateEvent
  17: PruneJobUpdateHistory pruneJobUpdateHistory
}

// The current schema version ID.  This should be incremented each time the
// schema is changed, and support code for schema migrations should be added.
const i32 CURRENT_SCHEMA_VERSION = 1

// Represents a series of local storage mutations that should be applied in a single atomic
// transaction.
struct Transaction {
  1: list<Op> ops
  2: i32 schemaVersion
}

struct StoredCronJob {
  3: api.JobConfiguration jobConfiguration
}

struct SchedulerMetadata {
  1: string frameworkId
  8: map<string, string> details
}

struct QuotaConfiguration {
  1: string role
  2: api.ResourceAggregate quota
}

// Represents a complete snapshot of local storage data suitable for restoring the local storage
// system to its state at the time the snapshot was taken.
struct Snapshot {

  // The timestamp when the snapshot was made in milliseconds since the epoch.
  1: i64 timestamp

  3: set<api.HostAttributes> hostAttributes
  4: set<api.ScheduledTask> tasks
  5: set<StoredCronJob> cronJobs
  6: SchedulerMetadata schedulerMetadata
  8: set<QuotaConfiguration> quotaConfigurations
  // 9: deleted
  10: set<StoredJobUpdateDetails> jobUpdateDetails
  //11: removed
  //12: removed
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

// A ScheduledTask with its assignedTask.task field set to null. Deserializers must fill in
// assignedTask.task with the TaskConfig identified by taskConfigId (which is an index into the
// DeduplicatedSnapshot's taskConfigs list).
struct DeduplicatedScheduledTask {
  1: api.ScheduledTask partialScheduledTask
  2: i32 taskConfigId
}

// A Snapshot that has had duplicate TaskConfig structs removed to save space. The
// partialSnapshot field is a normal Snapshot with the tasks field set to null. To create the
// full Snapshot deserializers must fill in this field with the result of recreating each
// partial task using the referenced entry in taskConfigs.
struct DeduplicatedSnapshot {
   // Snapshot with its tasks field unset.
   1: Snapshot partialSnapshot
   // ScheduledTasks that have had their assignedTask.task field replaced with an ID to save space.
   2: list<DeduplicatedScheduledTask> partialTasks
   // Ordered list of taskConfigs. The taskConfigId field of DeduplicatedScheduledTask is an index
   // into this.
   3: list<api.TaskConfig> taskConfigs
}

// A scheduler storage write-ahead log entry consisting of no-ops to skip over or else snapshots or
// transactions to apply.  Any entry type can also be chopped up into frames if the entry is too big
// for whatever reason.
union LogEntry {
  // The full state of the scheduler at some point-in-time. Transactions appearing before this
  // entry in the log can be ignored.
  1: Snapshot snapshot

  // An incremental update to apply to the scheduler storage.
  2: Transaction transaction

  // The value should be ignored - both true and false signal an equivalent no operation marker.
  3: bool noop;

  // A frame that can be reassembled with others to form a complete LogEntry.
  4: Frame frame

  // A LogEntry that is first serialized in the thrift binary format,
  // then compressed using the "deflate" compression format.
  // Deflated entries are expected to be un-framed.  They may be pieced together by multiple frames,
  // but the contents of the deflated entry should not be a Frame.
  5: binary deflatedEntry

  // The full state of the scheduler at some point-in-time, in a compact layout. Transactions
  // appearing before this entry in the log can be ignored.
  6: DeduplicatedSnapshot deduplicatedSnapshot
}

