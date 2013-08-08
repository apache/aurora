import unittest

import copy

from twitter.mesos.client.api import MesosClientAPI
from twitter.mesos.tools.dangerous_shard_mutator import (
    maybe_rewrite_job,
    maybe_rewrite_task,
    SmfdHadoopMigration,
    TaskMutator)

from gen.twitter.mesos.ttypes import (
    AssignedTask,
    ConfigRewrite,
    Identity,
    JobConfigRewrite,
    JobConfiguration,
    JobKey,
    RewriteConfigsRequest,
    ShardConfigRewrite,
    ShardKey,
    TaskConfig)

from mox import Mox


JOB_KEY = JobKey(role='role_name', environment='job_env', name='job_name')
IDENTITY = Identity(user='user_name', role=JOB_KEY.role)
SHARD_KEY = ShardKey(shardId=5, jobKey=JOB_KEY)


def make_config(task_blob):
  return TaskConfig(owner=IDENTITY,
                    environment=JOB_KEY.environment,
                    jobName=JOB_KEY.name,
                    shardId=SHARD_KEY.shardId,
                    thermosConfig=task_blob)


def make_task(task_blob, task_id='task_id'):
  return AssignedTask(
      taskId=task_id,
      task=make_config(task_blob))


def make_job(task_blob):
  return JobConfiguration(
      owner=IDENTITY,
      taskConfig=make_config(task_blob))


def expect_shard_rewritten(mock_scheduler, original_task, new_thermos_config):
  modified = copy.deepcopy(original_task)
  modified.thermosConfig = new_thermos_config
  rewrite = ConfigRewrite(shardRewrite=ShardConfigRewrite(
                          shardKey=SHARD_KEY,
                          oldTask=original_task,
                          rewrittenTask=modified))
  mock_scheduler.unsafe_rewrite_config(RewriteConfigsRequest(rewriteCommands=[rewrite]))


class TestDangerousRewriteConfigs(unittest.TestCase):
  def setUp(self):
    self.mox = Mox()
    self.mock_mutator = self.mox.CreateMock(TaskMutator)
    self.mock_scheduler = self.mox.CreateMock(MesosClientAPI)

  def tearDown(self):
    self.mox.UnsetStubs()
    self.mox.VerifyAll()

  def test_task_unchanged(self):
    task_blob = 'task data blob'
    self.mock_mutator.maybe_rewrite(task_blob).AndReturn(task_blob)

    self.mox.ReplayAll()

    maybe_rewrite_task(make_task(task_blob), self.mock_mutator, self.mock_scheduler)

  def test_mutator_sanity_check_fail(self):
    task_blob = 'task data blob'
    mutated_blob = 'mutated blob'
    self.mock_mutator.maybe_rewrite(task_blob).AndReturn(mutated_blob)
    self.mock_mutator.is_edit_distance_sane(8).AndReturn(False)

    self.mox.ReplayAll()

    maybe_rewrite_task(make_task(task_blob), self.mock_mutator, self.mock_scheduler)

  def test_mutation_too_big(self):
    task_blob = 'a' + ('really ' * 10) + 'big blob'
    mutated_blob = 'a tiny blob'
    self.mock_mutator.maybe_rewrite(task_blob).AndReturn(mutated_blob)

    self.mox.ReplayAll()

    maybe_rewrite_task(make_task(task_blob), self.mock_mutator, self.mock_scheduler)

  def test_rewrite_task(self):
    task_blob = 'original blob'
    mutated_blob = 'changed blob'
    original_task = make_task(task_blob)
    self.mock_mutator.maybe_rewrite(task_blob).AndReturn(mutated_blob)
    self.mock_mutator.is_edit_distance_sane(8).AndReturn(True)
    expect_shard_rewritten(self.mock_scheduler, original_task.task, mutated_blob)

    self.mox.ReplayAll()

    maybe_rewrite_task(original_task, self.mock_mutator, self.mock_scheduler)

  def test_rewrite_job(self):
    jobA = make_job('original blob A')
    task_blob = 'original blob B'
    mutated_blob = 'mutated blob B'
    jobB = make_job(task_blob)
    self.mock_mutator.should_inspect(jobA).AndReturn(False)
    self.mock_mutator.should_inspect(jobB).AndReturn(True)
    self.mock_mutator.maybe_rewrite(task_blob).AndReturn(mutated_blob)
    self.mock_mutator.is_edit_distance_sane(8).AndReturn(True)
    modified = copy.deepcopy(jobB)
    modified.taskConfig.thermosConfig = mutated_blob
    rewrite = ConfigRewrite(jobRewrite=JobConfigRewrite(oldJob=jobB, rewrittenJob=modified))
    self.mock_scheduler.unsafe_rewrite_config(RewriteConfigsRequest(rewriteCommands=[rewrite]))

    self.mox.ReplayAll()

    maybe_rewrite_job(jobA, self.mock_mutator, self.mock_scheduler)
    maybe_rewrite_job(jobB, self.mock_mutator, self.mock_scheduler)

  def test_smfd_hadoop_skips_non_cron(self):
    jobA = make_job('a cron job')
    jobA.cronSchedule = '* * * * *'
    jobB = make_job('original blob A')

    self.mox.ReplayAll()

    maybe_rewrite_job(jobA, SmfdHadoopMigration(), self.mock_scheduler)
    maybe_rewrite_job(jobB, SmfdHadoopMigration(), self.mock_scheduler)

  def test_smfd_hadoop_migration_noop(self):
    task_blob = 'original blob'

    self.mox.ReplayAll()

    maybe_rewrite_task(make_task(task_blob), SmfdHadoopMigration(), self.mock_scheduler)

  def test_smfd_hadoop_migration(self):
    old_uri = ('hftp://hadoop-scribe-nn.smfd.twitter.com:50070'
        '/user/mesos/packer/smfd-devel/packagedata/fe/913a85c0620d143636bedce02cfaa0')
    new_uri = ('hftp://hadoop-backups-nn.smfd.twitter.com:50070'
        '/user/mesos/packer/smfd-devel/packagedata/fe/913a85c0620d143636bedce02cfaa0')
    blob_template = '''hadoop fs -copyToLocal %s GraphicsMagick.tar.gz'''
    task_blob = blob_template % old_uri
    mutated_blob = blob_template % new_uri
    original_task = make_task(task_blob)
    expect_shard_rewritten(self.mock_scheduler, original_task.task, mutated_blob)

    two_copies_task = make_task(task_blob + '\n\n' + task_blob)
    two_copies_mutated = mutated_blob + '\n\n' + mutated_blob
    expect_shard_rewritten(self.mock_scheduler, two_copies_task.task, two_copies_mutated)

    self.mox.ReplayAll()

    mutator = SmfdHadoopMigration()
    maybe_rewrite_task(original_task, mutator, self.mock_scheduler)
    maybe_rewrite_task(two_copies_task, mutator, self.mock_scheduler)

  def test_smfd_hadoop_migration_edit_modulus(self):
    self.mox.ReplayAll()

    mutator = SmfdHadoopMigration()
    # 6 is the levenschtein edit distance between the two hadoop cluster names.
    assert mutator.is_edit_distance_sane(6)
    assert mutator.is_edit_distance_sane(12)
    assert mutator.is_edit_distance_sane(18)
    assert not mutator.is_edit_distance_sane(1)
    assert not mutator.is_edit_distance_sane(5)
    assert not mutator.is_edit_distance_sane(7)
