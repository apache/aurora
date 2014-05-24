/*
 *
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

namespace java org.apache.aurora.gen.comm
namespace py gen.apache.aurora.comm

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
