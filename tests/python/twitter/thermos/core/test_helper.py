import time

from twitter.common.quantity import Time
from twitter.thermos.core.helper import TaskRunnerHelper as TRH

from gen.twitter.thermos.ttypes import (
    ProcessStatus,
    RunnerHeader,
    RunnerState,
)

import mock
import psutil


USER1 = 'user1'
PID = 12345
CREATE_TIME = time.time()
PROCESS_NAME = 'my_process'
COORDINATOR_PID = 13337


def test_this_is_really_our_pid():
  process = mock.Mock(spec=psutil.Process, username=USER1, pid=PID, create_time=CREATE_TIME)
  assert TRH.this_is_really_our_pid(
      process,
      process.username,
      process.create_time)
  assert TRH.this_is_really_our_pid(
      process,
      process.username,
      process.create_time + TRH.MAX_START_TIME_DRIFT.as_(Time.SECONDS) - 1)
  assert not TRH.this_is_really_our_pid(
      process,
      'user2',
      process.create_time)
  assert not TRH.this_is_really_our_pid(
      process,
      process.username,
      process.create_time + TRH.MAX_START_TIME_DRIFT.as_(Time.SECONDS) + 1)
  assert not TRH.this_is_really_our_pid(
      process,
      process.username,
      process.create_time - (TRH.MAX_START_TIME_DRIFT.as_(Time.SECONDS) + 1))


TRH_PATH = 'twitter.thermos.core.helper.TaskRunnerHelper'
PSUTIL_PATH = 'psutil.Process'


def make_runner_state(cpid=COORDINATOR_PID, pid=PID, user=USER1, pname=PROCESS_NAME):
  return RunnerState(
    header=RunnerHeader(user=user),
    processes={
      pname: [
        ProcessStatus(
          fork_time=CREATE_TIME,
          start_time=CREATE_TIME,
          pid=pid,
          coordinator_pid=cpid,
          process=pname)
      ]
    }
  )


def test_scan_process():
  # TODO(jon): add more tests for successful cases; this really just looks for errors.

  assert TRH.scan_process(
      make_runner_state(cpid=None, pid=None), PROCESS_NAME) == (None, None, set())

  with mock.patch(PSUTIL_PATH) as p_mock:
    class WrappedNoSuchProcess(psutil.NoSuchProcess):
      # psutil.NoSuchProcess exception requires an argument, but mock doesn't support that.
      def __init__(self): pass
    p_mock.side_effect = WrappedNoSuchProcess
    assert TRH.scan_process(
        make_runner_state(cpid=None), PROCESS_NAME) == (None, None, set())
    assert TRH.scan_process(
        make_runner_state(pid=None), PROCESS_NAME) == (None, None, set())

  with mock.patch(PSUTIL_PATH) as p_mock:
    p_mock.side_effect = psutil.Error
    assert TRH.scan_process(
        make_runner_state(cpid=None), PROCESS_NAME) == (None, None, set())
    assert TRH.scan_process(
        make_runner_state(pid=None), PROCESS_NAME) == (None, None, set())

  with mock.patch(TRH_PATH) as trh_mock:
    trh_mock.this_is_really_our_pid.returns = False
    assert TRH.scan_process(
        make_runner_state(), PROCESS_NAME) == (None, None, set())

  with mock.patch(TRH_PATH) as trh_mock:
    trh_mock.this_is_really_our_pid.sideffect = psutil.Error
    assert TRH.scan_process(
        make_runner_state(), PROCESS_NAME) == (None, None, set())
