#!/usr/bin/env python2.6
import os
import subprocess
import sys
import time
from twitter.common import app
from twitter.common.net.tunnel import TunnelHelper
from twitter.mesos.clusters import Cluster

__author__ = 'William Farner'

REMOTE_USER = 'mesos'

BUILD_TARGET_CMDS = [
  './pants src/python/twitter/mesos/executor:thermos_executor',
  './pants src/python/twitter/mesos/executor:thermos_runner',
  './pants src/python/twitter/mesos/executor:gc_executor',
]

STAGE_HOST = 'nest2.corp.twitter.com'
STAGE_DIR = '~/release_staging'
DC_WILDCARD = '$dc'
CLUSTER_WILDCARD = '$cluster'
HDFS_BIN_DIR = '/mesos/pkg/mesos/bin'
HDFS_BIN_FILES = {
  'dist/thermos_executor.pex':  '%s/$cluster/thermos_executor.pex' % HDFS_BIN_DIR,
  'dist/gc_executor.pex':  '%s/$cluster/gc_executor.pex' % HDFS_BIN_DIR,
}

def get_cluster_dc():
  return Cluster.get(app.get_options().cluster).dc

def get_cluster_name():
  return Cluster.get(app.get_options().cluster).local_name

def maybe_run_command(runner, cmd):
  if app.get_options().verbose or not app.get_options().really_push:
    print '%s command: %s' % ('Executing' if app.get_options().really_push else 'Would run', ' '.join(cmd))
  if app.get_options().really_push:
    return runner(cmd)

def check_call(cmd):
  """Wrapper for subprocess.check_call."""
  maybe_run_command(subprocess.check_call, cmd)

def run_cmd(cmd):
  """Runs a command and returns its return code along with stderr/stdout tuple"""
  def fork_join(args):
    proc = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    output = proc.communicate()
    return proc.returncode, output
  return maybe_run_command(fork_join, cmd)

def ssh_target(host):
  return '%s@%s' % (REMOTE_USER, host)

def remote_call(host, cmd):
  return run_cmd(['ssh', ssh_target(host)] + cmd)

def cmd_output(cmd):
  """Runs a command and returns only its stdout"""
  result = run_cmd(cmd)
  if result:
    returncode, output = result
    return output[0].strip()
  else:
    return None

def check_output(cmd):
  """Stand-in for subprocess.check_output, added in python 2.7"""
  result = run_cmd(cmd)
  if result is not None:
    returncode, output = result
    assert returncode == 0, 'Command failed: "%s", output %s' % (' '.join(cmd), output)
    return output

def remote_check_call(host, cmd):
  check_output(['ssh', ssh_target(host)] + cmd)

def thermos_postprocess():
  import contextlib
  import zipfile
  with contextlib.closing(zipfile.ZipFile('dist/thermos_executor.pex', 'a')) as zf:
    zf.writestr('twitter/mesos/executor/resources/__init__.py', '')
    zf.write('dist/thermos_runner.pex', 'twitter/mesos/executor/resources/thermos_runner.pex')

def build():
  for build_target_cmd in BUILD_TARGET_CMDS:
    print 'Executing build target: %s' % build_target_cmd
    check_call(build_target_cmd.split(' '))
  thermos_postprocess()

def replace_hdfs_file(host, local_file, hdfs_path):
  HADOOP_CONF_DIR = '/etc/hadoop/hadoop-conf-%s' % get_cluster_dc()
  BASE_HADOOP_CMD = ['hadoop', '--config', HADOOP_CONF_DIR, 'fs']
  remote_call(host, BASE_HADOOP_CMD + ['-mkdir', os.path.dirname(hdfs_path)])
  remote_call(host, BASE_HADOOP_CMD + ['-rm', hdfs_path])
  remote_check_call(host, BASE_HADOOP_CMD + ['-put', local_file, hdfs_path])

def stage_build():
  # Finally stage the HDFS artifacts
  wildcards = {
    DC_WILDCARD: get_cluster_dc(),
    CLUSTER_WILDCARD: get_cluster_name()
  }
  for local_file, hdfs_target in HDFS_BIN_FILES.items():
    for wildcard, value in wildcards.items():
      local_file = local_file.replace(wildcard, value)
      hdfs_target = hdfs_target.replace(wildcard, value)
    print 'Sending local file from %s to HDFS %s' % (local_file, hdfs_target)
    remote_check_call(STAGE_HOST, ['mkdir', '-p', STAGE_DIR])
    stage_file = os.path.join(STAGE_DIR, os.path.basename(local_file))
    check_output(['scp', local_file, '%s:%s' % (ssh_target(STAGE_HOST), stage_file)])
    replace_hdfs_file(STAGE_HOST, stage_file, hdfs_target)

app.set_usage('%prog [options] tag')

cluster_list = Cluster.get_list()

app.add_option(
  '--cluster',
  type = 'choice',
  choices = cluster_list,
  dest='cluster',
  help='Cluster to deploy the scheduler in (one of: %s)' % ', '.join(cluster_list))

app.add_option(
  '-v',
  dest='verbose',
  default=False,
  action='store_true',
  help='Verbose logging. (default: %default)')

app.add_option(
  '--really_push',
  dest='really_push',
  default=False,
  action='store_true',
  help='Safeguard to prevent fat-fingering.  When false, only show commands but do not run them. '
       '(default: %default)')

def main(args, options):
  if not options.cluster:
    cluster_list = Cluster.get_list()
    print ('Please specify the cluster you would like to deploy to with\n\t--cluster %s'
           % cluster_list)
    return
  build()
  stage_build()

app.main()
