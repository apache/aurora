//  Process(t): =>
//    - runner: pid [which after setsid is session leader]
//    - child:  pid [whose ppid is runner.pid]
//
//    => writes checkpoint_root/job_uid/processes/{runner.pid}.ckpt
//       { { pid = pid, start_time = start_time }, ..., { stop_time = stop_time, return_code = return_code } }
//    => does does open/write/close for every write operation
//
//    death upon child finishing
//
//  Should the following happen
//
//    0) runner loses its underlying .ckpt:
//       means the parent somehow assumed it was lost or wants it to die, kill everything
//
//    1) runner dies but child lives
//       periodically poll for runner health
//       i) TaskRunner observes both runner.pid and child.pid -- if runner.pid goes away,
//          check for child.pid (do some accounting to make sure that pids are what we think
//          they are, obviously) -- kill child.pid, mark process LOST
//
//    2) child dies but runner lives
//       observed return_code
//       i) able to write before death
//          normal, write then exit [set to FINISHED/FAILED depending upon exit status]
//       ii) unable to write before death
//          TaskRunner observes both runner.pid and child.pid are missing,
//          marks process as LOST.
//
//    3) fork runner but never see {runner.pid}.ckpt -- kill {runner.pid} on a timeout.
//       => could there be a race condition where {runner.pid}.ckpt shows up after we've done a SIGKILL?
//
//  ProcessWatcher.add(t => runner.pid):
//    Observe {runner.pid}, and {child.pid} as known.
//    If at any point {runner.pid} goes away: slurp ckpt, if {child.pid} alive, kill & set LOST
//    If at any point {child.pid} goes away:
//       wait for {runner.pid} to die or 60 seconds, whichever comes first
//          slurp up {runner.pid}.ckpt
//          if conclusive, update state
//          if not, set LOST

namespace py gen.twitter.thermos

enum ProcessRunState {
  // normal state
  WAITING   = 0   // blocked on execution dependencies or footprint restrictions
  FORKED    = 1   // starting, need to wait for signal from Process that it's running
  RUNNING   = 2   // currently running
  FINISHED  = 3   // ProcessWatcher has finished and updated process state
  KILLED    = 4   // Killed by user action

  // abnormal states
  FAILED    = 5   // returncode != 0
  LOST      = 6   // the coordinator_pid either died or some condition caused us to lose it.
}

// Sent as a stream of diffs
struct ProcessState {
  // Sequence number, must be monotonically increasing for all
  // ProcessState messages for a particular process across all runs.
  1: i64             seq

  // Process name
  3: string          process

  5: ProcessRunState run_state

  // WAITING -> FORKED
 10: i32             coordinator_pid
 11: double          fork_time

  // FORKED -> RUNNING
  6: double          start_time
  7: i32             pid

  // RUNNING -> {FINISHED, FAILED, KILLED}
  8: double          stop_time
  9: i32             return_code

  // {FORKED, RUNNING} -> LOST nothing happens.  this ProcessState ceases to exist.
  // Doesn't count against the run total.
}

// See lib/thermos/task/common/ckpt.py for the reconstruction logic.

enum TaskRunState {
  ACTIVE   = 0
  SUCCESS  = 1
  FAILED   = 2
  KILLED   = 3
}

enum TaskState {
  ACTIVE   = 0
  SUCCESS  = 1
  FAILED   = 2
  KILLED   = 3
}

struct ProcessHistory {
  1: TaskRunState state
  2: string process
  3: list<ProcessState> runs
}

// The first framed message in the Ckpt stream.
struct RunnerHeader {
  1: string task_id
  2: i64    launch_time_ms
  3: string sandbox
  4: string hostname
  5: string user
}

struct TaskAllocatedPort {
  1: string port_name
  2: i32 port
}

struct TaskRunStateUpdate {
  1: string       process
  2: TaskRunState state
}

struct TaskStatus {
  1: TaskState state
  2: i64       timestamp_ms
  3: i32       runner_pid
}

union RunnerCkpt {
  1: RunnerHeader       runner_header
  2: ProcessState       process_state
  3: TaskAllocatedPort  allocated_port
  4: TaskRunStateUpdate history_state_update
  5: TaskStatus         status_update
}

struct RunnerState {
  1: RunnerHeader header
  2: list<TaskStatus> statuses
  3: map<string, ProcessHistory> processes
  4: map<string, i32> ports
}
