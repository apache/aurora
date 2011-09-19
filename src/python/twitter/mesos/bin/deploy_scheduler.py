#!/usr/bin/env python2.6
import os
import subprocess
import time
from time import gmtime, strftime
from optparse import OptionParser

from git import *

from twitter.mesos import clusters

__author__ = 'William Farner'

REMOTE_USER = 'mesos'

TEST_CMD = './pants %s clean-all test'
TEST_TARGETS = ['tests/java/com/twitter/mesos:all-tests!']

BUILD_DEPLOY_CMD = './pants %s zip'
BUILD_TARGETS = [
  'src/java/com/twitter/mesos/scheduler!',
  'src/java/com/twitter/mesos/executor!',
]

STAGE_DIR = '~/release_staging'

SCHEDULER_PACKAGE = 'mesos-scheduler.zip'
BUILD_SCHEDULER_PACKAGE_PATH = 'dist/%s' % SCHEDULER_PACKAGE
BUILD_SCHEDULER_JAR_PATH = 'dist/mesos-scheduler-bundle/mesos-scheduler.jar'
STAGED_PACKAGE_PATH = '%s/%s' % (STAGE_DIR, SCHEDULER_PACKAGE)

DC_WILDCARD = '$dc'
CLUSTER_WILDCARD = '$cluster'
HDFS_BIN_DIR = '/mesos/pkg/mesos/bin'
HDFS_BIN_FILES = {
  'mesos/scripts/executor.sh': '%s/$cluster/$dc-$cluster-executor.sh' % HDFS_BIN_DIR,
  'dist/mesos-executor.zip':  '%s/$cluster/mesos-executor.zip' % HDFS_BIN_DIR,
}

MESOS_HOME = '/usr/local/mesos'
LIVE_BUILD_PATH = '%s/current' % MESOS_HOME
RELEASES_DIR = '%s/releases' % MESOS_HOME

SCHEDULER_HTTP = 'http://localhost:8081'

options = None

def get_cluster_dc():
  return clusters.get_dc(options.cluster)


def get_cluster_name():
  return clusters.get_local_name(options.cluster)


def get_scheduler_machine():
  return clusters.get_scheduler_host(options.cluster)


def read_bool_stdin(prompt, default=None):
  if default is not None:
    if default:
      prompt = '%s [y] ' % prompt
    else:
      prompt = '%s [n] ' % prompt
  while True:
    result = raw_input('%s ' % prompt).lower()
    if result:
      return result in ["yes", "y", "true", "t", "1"]
    elif default is not None:
      return default
    else:
      print "I'll keep asking until you answer!"


def maybe_run_command(runner, cmd):
  if options.verbose or not options.really_push:
    print '%s command: %s' % ('Executing' if options.really_push else 'Would run', ' '.join(cmd))
  if options.really_push:
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


def ssh_target():
  return '%s@%s' % (REMOTE_USER, get_scheduler_machine())


def remote_call(cmd):
  return run_cmd(['ssh', ssh_target()] + cmd)


def fetch_scheduler_http(endpoint):
  result = remote_call(['curl', '--silent', '%s/%s' % (SCHEDULER_HTTP, endpoint)])
  if result is not None:
    return result[1][0].strip()


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


def remote_check_call(cmd):
  check_output(['ssh', ssh_target()] + cmd)


def build():
  for test_target in TEST_TARGETS:
    print 'Executing test target: %s' % test_target
    check_call((TEST_CMD % test_target).split(' '))
  for build_target in BUILD_TARGETS:
    print 'Executing build target: %s' % build_target
    check_call((BUILD_DEPLOY_CMD % build_target).split(' '))


def find_current_build():
  # the linux machines do not have realpath installed - this is at least portable
  command = [
    'ssh',
    ssh_target(),
    """"python -c 'import os; print os.path.realpath(\\"%s\\")'" """ % LIVE_BUILD_PATH
  ]
  # TODO(John Sirois): get this to work via remote_call
  result = maybe_run_command(lambda cmd: os.popen(' '.join(cmd)).read(), command)
  if result:
    current_build = result.strip()
    if current_build == LIVE_BUILD_PATH:
      current_build = None
    print 'Found current build: %s' % current_build
    return current_build


def replace_hdfs_file(local_file, hdfs_path):
  HADOOP_CONF_DIR = '/etc/hadoop/hadoop-conf-%s' % get_cluster_dc()
  BASE_HADOOP_CMD = ['hadoop', '--config', HADOOP_CONF_DIR, 'fs']

  remote_call(BASE_HADOOP_CMD + ['-mkdir', os.path.dirname(hdfs_path)])
  remote_call(BASE_HADOOP_CMD + ['-rm', hdfs_path])
  remote_check_call(BASE_HADOOP_CMD + ['-put', local_file, hdfs_path])


def stage_build():
  result = cmd_output(['bash', '-c',
    'unzip -c %s build.properties | grep build.git.revision' % BUILD_SCHEDULER_JAR_PATH
  ])
  if options.really_push:
    _, sha = result.split('=')
  else:
    sha = '[sha]'
  release_scheduler_path = '%s/%s-%s' % (RELEASES_DIR, strftime("%Y%m%d%H%M%S", gmtime()), sha)

  print 'Staging the build on the scheduler machine at: %s' % release_scheduler_path
  remote_check_call(['mkdir', '-p', STAGE_DIR])
  check_output(['scp', BUILD_SCHEDULER_PACKAGE_PATH, '%s:%s' % (ssh_target(), STAGED_PACKAGE_PATH)])
  remote_check_call(['bash', '-c',
    '"mkdir -p %(release_dir)s &&'
    ' unzip -d %(release_dir)s %(staged_package)s &&'
    ' chmod +x %(release_dir)s/scripts/*.sh"' % {
      'release_dir': release_scheduler_path,
      'staged_package': STAGED_PACKAGE_PATH,
    },
  ])
  return release_scheduler_path


def set_live_build(build_path):
  wildcards = {
    DC_WILDCARD: get_cluster_dc(),
    CLUSTER_WILDCARD: get_cluster_name()
  }
  for local_file, hdfs_target in HDFS_BIN_FILES.items():
    for wildcard, value in wildcards.items():
      local_file = local_file.replace(wildcard, value)
      hdfs_target = hdfs_target.replace(wildcard, value)
    print 'Sending local file from %s to HDFS %s' % (local_file, hdfs_target)
    stage_file = os.path.join(STAGE_DIR, os.path.basename(local_file))
    check_output(['scp', local_file, '%s:%s' % (ssh_target(), stage_file)])
    replace_hdfs_file(stage_file, hdfs_target)

  print 'Linking the new build on the scheduler'
  remote_check_call(['bash', '-c',
    '"rm -f %(live_build)s &&'
    ' ln -s %(build)s %(live_build)s"' % {
      'live_build': LIVE_BUILD_PATH,
      'build': build_path
    }
  ])


def start_scheduler():
  print 'Starting the scheduler'
  remote_check_call(['sudo', 'monit', 'start', 'mesos-scheduler'])
  if options.really_push:
    print 'Waiting for the scheduler to start'
    time.sleep(5)


def get_scheduler_uptime_secs():
  vars = fetch_scheduler_http('vars')
  if options.really_push:
    assert vars is not None, 'Failed to fetch vars from scheduler'
  elif vars is None:
    return
  for var in vars.split('\n'):
    keyValue = var.split(' ')
    if keyValue[0] == 'jvm_uptime_secs':
      return int(keyValue[1])


def is_scheduler_healthy():
  if options.really_push:
    return fetch_scheduler_http('health') == 'OK'
  else:
    return True


def stop_scheduler():
  print 'Stopping the scheduler'
  print 'Temporarily disabling monit for the scheduler'
  remote_check_call(['sudo', 'monit', 'unmonitor', 'mesos-scheduler'])
  fetch_scheduler_http('quitquitquit')
  print 'Waiting for scheduler to stop cleanly'
  if options.really_push:
    time.sleep(5)
  print 'Stopping scheduler via monit'
  remote_check_call(['sudo', 'monit', 'stop', 'mesos-scheduler'])


def watch_scheduler():
  print 'Watching scheduler'
  started = False
  watch_start = time.time()
  start_detected_at = 0
  last_uptime = 0
  # Wait at most three minutes.
  while started or (time.time() - watch_start) < 180:
    if is_scheduler_healthy():
      uptime = get_scheduler_uptime_secs()
      if not options.really_push:
        print 'Skipping further health checks, since we are not pushing.'
        return True
      print 'Up and healthy for %s seconds' % uptime

      if started:
        if uptime < last_uptime:
          print 'Detected scheduler process restart after update (uptime %s)!' % uptime
          return False
        elif time.time() - start_detected_at > 45:
          print 'Scheduler has been up for at least 45 seconds'
          return True
      else:
        start_detected_at = time.time()

      started = True
      last_uptime = uptime
    elif started:
      print 'Scheduler stopped responding to health checks!'
      return False
    time.sleep(2)
  return False


def rollback(rollback_build):
  print 'Initiating rollback'
  stop_scheduler()
  set_live_build(rollback_build)
  start_scheduler()


def main():
  parser = OptionParser()
  parser.add_option(
    '-v',
    dest='verbose',
    default=False,
    action='store_true',
    help='Verbose logging. (default: %default)')

  cluster_list = list(clusters.get_clusters())
  cluster_list.sort()
  parser.add_option(
    '--cluster',
    type = 'choice',
    choices = cluster_list,
    dest='cluster',
    help='Cluster to deploy the scheduler in (one of: %s)' % ', '.join(cluster_list))

  parser.add_option(
    '--skip_build',
    dest='skip_build',
    default=False,
    action='store_true',
    help='Skip build and test, use the existing build. (default: %default)')
  parser.add_option(
    '--really_push',
    dest='really_push',
    default=False,
    action='store_true',
    help='Safeguard to prevent fat-fingering.  When false, only show commands but do not run them. '
         '(default: %default)')

  global options
  (options, args) = parser.parse_args()

  if not options.really_push:
    print '****************************************************************************************'
    print 'You are running in pretend mode.  None of the commands are actually executed!'
    print 'If you wish to push, add command line arg --really_push'
    print '****************************************************************************************'

  if not options.cluster:
    cluster_list = list(clusters.get_clusters())
    cluster_list.sort()
    print ('Please specify the cluster you would like to deploy to with\n\t--cluster %s'
           % cluster_list)
    return

  deploy_branch = clusters.get_deploy_branch(options.cluster)
  repo = Repo()
  if repo.active_branch.name != deploy_branch:
    if not read_bool_stdin(
        'The standard deploy branch for the %s cluster is %s, currently on branch %s, are you sure '
        'you want to continue?' % (options.cluster, deploy_branch, repo.active_branch.name), False):
      return

  if options.skip_build:
    print 'Warning - skipping build, using existing build at %s' % BUILD_SCHEDULER_PACKAGE_PATH
  else:
    build()

  current_build = find_current_build()
  new_build = stage_build()
  stop_scheduler()
  set_live_build(new_build)
  start_scheduler()
  if not watch_scheduler():
    print 'scheduler not healthy'
    if current_build:
      rollback(current_build)
    else:
      print 'Push failed - no previous builds to roll back to.'
  elif options.really_push:
    print 'Push successful!'
  else:
    print 'Fake push completed'

if __name__ == '__main__':
  main()
