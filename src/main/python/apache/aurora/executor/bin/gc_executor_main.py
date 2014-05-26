#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Command-line entry point to the Thermos GC executor

This module wraps the Thermos GC executor into an executable suitable for launching by a Mesos
slave.

"""

import mesos
from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.metrics.sampler import DiskMetricWriter

from apache.aurora.executor.executor_detector import ExecutorDetector
from apache.aurora.executor.gc_executor import ThermosGCExecutor
from apache.thermos.common.path import TaskPath

app.configure(debug=True)


# locate logs locally in executor sandbox
LogOptions.set_simple(True)
LogOptions.set_disk_log_level('DEBUG')
LogOptions.set_log_dir(ExecutorDetector.LOG_PATH)


def proxy_main():
  def main():
    # Create executor stub
    thermos_gc_executor = ThermosGCExecutor(checkpoint_root=TaskPath.DEFAULT_CHECKPOINT_ROOT)
    thermos_gc_executor.start()

    # Start metrics collection
    metric_writer = DiskMetricWriter(thermos_gc_executor.metrics, ExecutorDetector.VARS_PATH)
    metric_writer.start()

    # Create driver stub
    driver = mesos.MesosExecutorDriver(thermos_gc_executor)

    # Start GC executor
    driver.run()

    log.info('MesosExecutorDriver.run() has finished.')

  app.main()
