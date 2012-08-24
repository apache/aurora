#!/usr/bin/env python2.6
"""
A program (designed to be run as a mesos task) to reboot a given user/job
pair. The program starts by querying the scheduler for all tasks belonging
to the specified user/job pair.

Once found, we reboot the first few tasks in canary mode (i.e. one at a
time, with no tolerance for failures). Each task must have a minimum uptime
before moving on.

After canary mode is finished, we reboot the remainder of the tasks in "main"
mode, which can be parallelized. We terminate the process if less than
a specific fraction of rebooted tasks remain up for a given time.

See the flags defined in main() for all available options.
"""

__author__ = 'Alex Roetter'

from optparse import OptionParser
import getpass
import logging
import Queue
import sys
import threading
import time
import getpass

from twitter.common import log
from twitter.mesos import scheduler_client
from gen.twitter.mesos.ttypes import *

SCHEDULER_ZK_PATH = "/twitter/service/mesos-scheduler"

# How long to sleep, in seconds, between polls to see if a task has
# restarted yet. A task is considered restarted when a new task exists
# with the restarted task as its ancestor (see the ancestorId field of
# the TrackedTask thrift message).
WAITING_FOR_TASK_RESTART_POLL_PERIOD = 5

# How many times should I try to query the scheduler for active tasks before
# aborting the program?
NUM_ACTIVE_TASK_QUERY_ATTEMPTS = 10

# How long to sleep, in seconds, between polls of a task's status. We
# poll a task's status to determine if the task has been alive long enough
# to consider the task properly restarted
TASK_STATUS_POLL_PERIOD = 5


class JobRestarter(object):
  """ Class that handles starting the given user's job. Runs in canary mode
  first, and after that in "main" mode, which parallelizes restarts, stopping
  if a certain percentage or greater fail.
  """


  def __init__(self, user, job, num_canaries, max_task_restart_time,
               task_required_uptime, task_restart_parallelism,
               task_restart_period, reqd_restart_success_rate):
    # Constants
    self.user = user
    self.job = job
    self.num_canaries = num_canaries
    self.max_task_restart_time = max_task_restart_time
    self.task_required_uptime = task_required_uptime
    self.task_restart_parallelism = task_restart_parallelism
    self.task_restart_period = task_restart_period
    self.reqd_restart_success_rate = reqd_restart_success_rate

    # shared state across many threads, and the lock protecting that state
    self.num_restart_failures = 0
    self.num_restart_successes = 0
    self.lock = threading.RLock()

    # has its own internal lock. Set when worker threads should exit, due
    # to an error / too high of a failure rate, etc.
    self.should_abort = threading.Event()

    # Thread local storage
    self.thread_local = threading.local()


  def restart_tasks_or_die(self, task_list, debug_name, num_threads,
                           reqd_restart_success_rate,
                           per_worker_restart_period):
    """ Restart all the tasks specified in task_list.

    The number of worker threads to use, required success percentage for
    task restarts, and avg period (in seconds) between restarts can be
    specified. debug_name is usually one of "Canary", "Main", and is used just
    for logging messages.
    """
    queue = Queue.Queue()
    for task in task_list:
      queue.put(task.scheduledTask.assignedTask.taskId)

    workers = []
    for i in xrange(0, num_threads):
      t = threading.Thread(
        name="%s #%d" % (debug_name, i),
        target=self.restart_task_thread_worker,
        args=(queue, reqd_restart_success_rate,
              per_worker_restart_period))
      workers.append(t)
      t.start()

    log.info("Waiting for %s threads to terminate..." % debug_name)
    for t in workers:
      t.join()
      log.info("Joined on thread %s" % t.name)

    if self.should_abort.is_set():
      log.critical("%s threads failed. Aborting restart." % debug_name)
      sys.exit(1)


  def restart_job(self):
    """ The main work, restarting all the job's tasks, or aborting on an error.
    """

    sc = scheduler_client.SchedulerClient(SCHEDULER_ZK_PATH, False)
    self.thread_local.client = sc.get_thrift_client()

    statuses = self.get_active_task_statuses()
    if statuses is None:
      log.critical(
        "No active tasks for user=%s job=%s. Done" % (self.user, self.job))
      sys.exit(0)
    else:
      log.info("Got back %d tasks for user=%s job=%s" % (
          len(statuses), self.user, self.job))

    canary_tasks = statuses[:self.num_canaries]
    main_tasks = statuses[self.num_canaries:]

    log.info("Entering canary mode, rebooting tasks serially.")
    self.restart_tasks_or_die(
      canary_tasks, "Canary", 1, .99, self.task_restart_period)

    log.info("%d tasks to reboot in main reboot mode." %
             len(main_tasks))

    # We want the workers to restart one task every
    # self.task_restart_period seconds (on average).
    per_worker_restart_period = (self.task_restart_parallelism *
                                 self.task_restart_period)
    self.restart_tasks_or_die(
      main_tasks, "Main", self.task_restart_parallelism,
      self.reqd_restart_success_rate, per_worker_restart_period)

    log.info("Success!")


  def task_restart_should_timeout(self):
    """ Should give up on restarting the current task and mark it as failed?
    """
    if self.max_task_restart_time == -1:
      return False

    elapsed = time.time() - self.thread_local.start_time
    return elapsed > self.max_task_restart_time


  def get_restarted_task_id(self, task_id):
    """ Figure out the new task id for a task that is being rebooted.

    Given a task_id that the scheduler is rebooting, return the
    newly created task's id, i.e. the one whose ancestor task id is
    task_id. Return None if we can't find the new task.
    """

    statuses = self.get_active_task_statuses()
    if statuses is None:
      return None
    for task in statuses:
      if task.scheduledTask.ancestorId == task_id:
        log.info("task %d has been restarted as %d" % (task_id, task.taskId))
        return task.taskId
    # Didn't find the task
    return None


  def has_been_alive_long_enough(self, task_id, task_events,
                                 reqd_lifetime_secs):
    """ Given a list of task_events of the type found in a TrackedTask, return
    true if the job has been running for at least lifetime_secs.
    """
    # Look backwards over its statuses to figure out how long job has
    # been alive for.

    # TODO(William Farner): replace this with code similar to:
    #
    # def ...(self, task_id, latest_task_event...):
    #  if (latest_task_event.status is not RUNNING:
    #    return False
    #  else:
    #    return time.time() - int(latest_task_event.timestamp / 1000)
    #      > reqd_lifetime_secs;
    #
    # Once you have fixed the bug where multiple RUNNING events are created.

    oldest_running_event = None
    for e in reversed(task_events):
      # the first (i.e. most recent) non-running event is cause to stop
      # iterating backwards though events for this task.
      event_time_secs = int(e.timestamp / 1000)

      if e.status is not ScheduleStatus.RUNNING:
        break

      if (oldest_running_event is None or
          event_time_secs < oldest_running_event):
          oldest_running_event = event_time_secs

    if oldest_running_event is None:
      log.info("Task %d hasn't started yet." % task_id)
      return False

    lifetime = time.time() - oldest_running_event
    log.info("Task %d has been running for %d secs." % (task_id, lifetime))
    return lifetime > reqd_lifetime_secs


  def wait_until_task_is_old_enough(self, task_id):
    """ Return when a given task has been alive for long enough that
    we consider it restarted.

    This time is configurable via a command line flag.
    If we timeout while waiting for a long enough task uptime, we fail.
    Returns a boolean, True on success, False on failure/timeout.
    """
    # TODO(William Farner): check for /health during this process.
    #
    # TODO(Alex Roetter): doesn't currently catch failures, but will notice
    # them indirectly as the task won't live for long enough before timing out.
    # Consider counting failures, and failing explicitly if it exceeds a
    # certain number.
    while not self.task_restart_should_timeout():
      query = TaskQuery()
      query.taskIds = set()
      query.taskIds.add(task_id)
      resp = self.thread_local.client.getTasksStatus(query)
      if resp.responseCode != ResponseCode.OK or len(resp.taskStatuses) != 1:
        log.info("Can't query status of task %d. %s (message: %s)" % (
            task_id, ResponseCode._VALUES_TO_NAMES[resp.responseCode],
            resp.message))
      else:
        if self.has_been_alive_long_enough(
          task_id, resp.taskStatuses[0].scheduledTask.taskEvents,
          self.task_required_uptime):
          log.info("Successful restart of task %d." % task_id)
          return True
      time.sleep(TASK_STATUS_POLL_PERIOD)

    log.info("Timed out waiting for restart of task %d" % task_id)
    return False


  def restart_one_task(self, task_id):
    """ Restart the given task id, returning a boolean success flag.

    We fail if we can't communicate with the scheduler, the scheduler
    never reschedules the task, or we timeout before the task has been
    up long enough.
    """
    log.info("Restarting one task id=%d" % task_id)
    # store the start time for this task restart in thread-local storage.

    # send restart command to server.
    resp = self.thread_local.client.restartTasks(set([task_id]))
    log.info("Told scheduler to restart task %d -> %s (message: %s)" % (
        task_id, ResponseCode._VALUES_TO_NAMES[resp.responseCode],
        resp.message))
    if resp.responseCode != ResponseCode.OK:
      log.error("Scheduler Restart RPC for task %d failed." % task_id)
      return False

    new_task_id = self.get_restarted_task_id(task_id)
    if new_task_id is None:
      log.error("Timed out waiting for task %d to restart." % task_id)
      return False

    return self.wait_until_task_is_old_enough(new_task_id)


  def restart_task_thread_worker(self, queue, reqd_success_fraction,
                                 per_worker_restart_period):
    """
    body of a thread which will read task-ids off a queue and restart them.

    This thread will restart tasks at most once per per_worker_restart_period
    (secs), and fail if the global (across all workers) success rate falls
    below reqd_success_fraction. In that case, we signal the shared event
    self.should_abort, which will cause other workers to terminate
    """
    # Set up a per-worker thread client to the scheduler, in case
    # it's not reentrant.
    # TODO(Alex Roetter): confirm it's reentrant and optimize this away if so.
    sc = scheduler_client.SchedulerClient(SCHEDULER_ZK_PATH, False)
    self.thread_local.client = sc.get_thrift_client()

    while not self.should_abort.is_set():
      log.info("%d tasks remaining to restart." % queue.qsize())
      try:
        task_id = queue.get(block=False)
      except Queue.Empty:
        log.info("Terminating as queue is empty.")
        break

      log.info("Acquired task %d to reboot." % task_id)
      self.thread_local.start_time = time.time()

      success = self.restart_one_task(task_id)

      with self.lock:
        if success:
          self.num_restart_successes += 1
        else:
          self.num_restart_failures += 1
        success_rate = self.num_restart_successes / float(
          self.num_restart_successes + self.num_restart_failures)

      if success_rate < reqd_success_fraction:
        log.info("Success rate dropped below threshold %f" %
                 reqd_success_fraction)
        self.should_abort.set()
      else:
        time_so_far = int(time.time() - self.thread_local.start_time)
        log.info("Sleeping...")
        time.sleep(max(0, per_worker_restart_period - time_so_far))


  def get_active_task_statuses(self):
    """ Return a list of TrackedTask thrift messages, one per active
    task. An active task is one that is PENDING, STARTING, or RUNNING.

    returns None if there was a failure communicating with the scheduler. """

    query = TaskQuery()
    query.owner = self.user
    query.jobName = self.job

    query.statuses = set()
    query.statuses.add(ScheduleStatus.PENDING)
    query.statuses.add(ScheduleStatus.STARTING)
    query.statuses.add(ScheduleStatus.RUNNING)

    for i in xrange(0, NUM_ACTIVE_TASK_QUERY_ATTEMPTS):
      resp = self.thread_local.client.getTasksStatus(query)
      if resp.responseCode is not ResponseCode.OK:
        log.warning("Couldn't lookup tasks [%s]. Retrying..." % e)
        time.sleep(3)
      else:
        return resp.tasks

    log.error("Couldn't lookup tasks. Quitting...")
    return None


def main(args):
  """ Parse the command line, and start a JobRestarter to do the real work.
  """
  parser = OptionParser()
  parser.add_option(
    "-u",
    dest="user",
    default=getpass.getuser(),
    help="The user owning the job to restart. (default: %default)")
  parser.add_option(
    "--num_canaries",
    dest="num_canaries",
    default=1,
    type="int",
    help="How many tasks to canary (serial restarts, no failures allowed)." +
    " (default: %default)")
  parser.add_option(
    "--max_task_restart_time",
    dest="max_task_restart_time",
    default=-1,
    type="int",
    help="Maximum amount of time to wait (in secs) for a task to restart," +
    " before marking it as failed. (default: %default)")
  parser.add_option(
    "--task_required_uptime",
    dest="task_required_uptime",
    default=120,
    type="int",
    help="Number of seconds a restarted task must be running for it to be" +
    " considered successfully restarted. (default: %default)")
  parser.add_option(
    "--task_restart_parallelism",
    dest="task_restart_parallelism",
    default=3,
    type="int",
    help="How many task restarts do we do simultaneously, once out of canary" +
    " mode? (default: %default)")
  parser.add_option(
    "--task_restart_period",
    dest="task_restart_period",
    default=3,
    type="int",
    help="How long (on average) between task restarts (seconds)." +
    " (default: %default)")
  parser.add_option(
    "--reqd_restart_success_rate",
    dest="reqd_restart_success_rate",
    default=0.8,
    type="float",
    help="Fraction of restarts that must be successful, or else the" +
    " rolling restart is aborted. (default: %default)")

  (options, args) = parser.parse_args()

  if len(args) != 1:
    parser.error("Required argument job name missing.")
  jobname = args[0]

  restarter = JobRestarter(
    options.user,
    jobname,
    num_canaries=options.num_canaries,
    max_task_restart_time=options.max_task_restart_time,
    task_required_uptime=options.task_required_uptime,
    task_restart_parallelism=options.task_restart_parallelism,
    task_restart_period=options.task_restart_period,
    reqd_restart_success_rate=options.reqd_restart_success_rate)

  restarter.restart_job()


if __name__ == "__main__":
  main(sys.argv)
