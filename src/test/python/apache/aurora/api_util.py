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

from apache.aurora.client.api.scheduler_client import SchedulerProxy

from gen.apache.aurora.api import ReadOnlyScheduler


class SchedulerThriftApiSpec(ReadOnlyScheduler.Iface):
  """
  A concrete definition of the thrift API used by the client. Intended primarily to be used as a
  spec definition for unit testing, since the client effectively augments function signatures by
  allowing callers to omit the session argument (in SchedulerProxy). These signatures should be
  identical to those in AuroraAdmin.Iface, with the session removed.
  """

  def setQuota(self, ownerRole, quota):
    pass

  def forceTaskState(self, taskId, status):
    pass

  def performBackup(self):
    pass

  def listBackups(self):
    pass

  def stageRecovery(self, backupId):
    pass

  def queryRecovery(self, query):
    pass

  def deleteRecoveryTasks(self, query):
    pass

  def commitRecovery(self):
    pass

  def unloadRecovery(self):
    pass

  def startMaintenance(self, hosts):
    pass

  def drainHosts(self, hosts):
    pass

  def maintenanceStatus(self, hosts):
    pass

  def endMaintenance(self, hosts):
    pass

  def snapshot(self):
    pass

  def createJob(self, description):
    pass

  def scheduleCronJob(self, description):
    pass

  def descheduleCronJob(self, job):
    pass

  def startCronJob(self, job):
    pass

  def restartShards(self, job, shardIds):
    pass

  def killTasks(self, jobKey, instances, message):
    pass

  def addInstances(self, key, count):
    pass

  def replaceCronTemplate(self, config):
    pass

  def startJobUpdate(self, request, message):
    pass

  def pauseJobUpdate(self, jobKey, message):
    pass

  def resumeJobUpdate(self, jobKey, message):
    pass

  def abortJobUpdate(self, jobKey, message):
    pass

  def pulseJobUpdate(self, updateId):
    pass


class SchedulerProxyApiSpec(SchedulerThriftApiSpec, SchedulerProxy):
  """
  A concrete definition of the API provided by SchedulerProxy.
  """

  def getTasksStatus(self, query, retry=True):
    pass

  def getTasksWithoutConfigs(self, query, retry=True):
    pass

  def getJobs(self, ownerRole, retry=True):
    pass

  def getQuota(self, ownerRole, retry=True):
    pass

  def populateJobConfig(self, description, retry=True):
    pass

  def getJobUpdateSummaries(self, jobUpdateQuery, retry=True):
    pass

  def getJobUpdateDetails(self, key, query, retry=True):
    pass

  def getJobUpdateDiff(self, request, retry=True):
    pass

  def getTierConfigs(self, retry=True):
    pass

  def queryRecovery(self, query, retry=True):
    pass

  def maintenanceStatus(self, hosts, retry=True):
    pass

  def startJobUpdate(self, request, message, retry=True):
    pass

  def restartShards(self, job, shardIds, retry=True):
    pass

  def url(self):
    pass
