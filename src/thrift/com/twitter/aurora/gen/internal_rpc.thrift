// Copyright 2010 Twitter, Inc.
namespace java com.twitter.aurora.gen.comm
namespace py gen.twitter.aurora.comm

include "api.thrift"

// Thrift interface to define the communication between the scheduler and executor.

// Message sent from the executor to the scheduler, indicating that sandboxes
// for terminated tasks were deleted due to disk exhaustion.
struct DeletedTasks {
  1: set<string> taskIds
}

// Message sent from the executor to the scheduler.
// TODO(wfarner): Consider renaming to be more clear.
union SchedulerMessage {
  4: DeletedTasks deletedTasks
}

// Message sent from the scheduler to the executor, indicating that some
// task history associated with the host may have been purged, and the
// executor should only retain tasks associated with the provided tasks IDs.
struct AdjustRetainedTasks {
  1: set<string> retainedTaskIds  // DEPRECATED - All task IDs that the executor should retain.
  2: map<string, api.ScheduleStatus> retainedTasks  // All tasks that the executor should
                                                              // retain, and their statuses.
}

// Message sent from the scheduler to the executor.
// TODO(wfarner): Consider renaming to be more clear.
union ExecutorMessage {
  4: AdjustRetainedTasks adjustRetainedTasks
}

struct TaskResourceSample {
  // dynamic
 13: i64 microTimestamp

  // static
  1: double reservedCpuRate
  2: double reservedRamBytes
  3: double reservedDiskBytes

  // dynamic
  4: double cpuRate
  5: double cpuUserSecs
  6: double cpuSystemSecs
  7: i16 cpuNice
  8: i64 ramRssBytes
  9: i64 ramVssBytes
 10: i16 numThreads
 11: i16 numProcesses
 12: i64 diskBytes
}
