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

namespace java org.apache.aurora.gen.comm
namespace py gen.apache.aurora.comm

include "api.thrift"

// Thrift interface to define the communication between the scheduler and executor.

// Message sent from the scheduler to the executor, indicating that some
// task history associated with the host may have been purged, and the
// executor should only retain tasks associated with the provided tasks IDs.
struct AdjustRetainedTasks {
  2: map<string, api.ScheduleStatus> retainedTasks  // All tasks that the executor should
                                                    // retain, and their statuses.
}
