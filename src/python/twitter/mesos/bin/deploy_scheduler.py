#!/usr/bin/env python2.6
import os
import subprocess
import sys
import time

from git import *

from optparse import OptionParser
from time import gmtime, strftime

from twitter.common import app
from twitter.mesos.clusters import Cluster
from twitter.mesos.tunnel_helper import TunnelHelper

__author__ = 'William Farner'

REMOTE_USER = 'mesos'

TEST_CMDS = ['./pants goal clean-all test tests/java/com/twitter/mesos:all']

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
  'dist/process_scraper.pex':  '%s/$cluster/process_scraper.pex' % HDFS_BIN_DIR,
  'dist/thermos_executor.pex':  '%s/$cluster/thermos_executor.pex' % HDFS_BIN_DIR,
  'dist/gc_executor.pex':  '%s/$cluster/gc_executor.pex' % HDFS_BIN_DIR,
}

MESOS_HOME = '/usr/local/mesos'
LIVE_BUILD_PATH = '%s/current' % MESOS_HOME
RELEASES_DIR = '%s/releases' % MESOS_HOME

SCHEDULER_HTTP = 'http://localhost:8081'


def get_cluster():
  return Cluster.get(app.get_options().cluster)


def get_cluster_dc():
  return get_cluster().dc


def get_cluster_name():
  return get_cluster().local_name


def get_scheduler_role():
  return get_cluster().scheduler_role


def cluster_is_colonized():
  return get_cluster().is_colonized


def get_hadoop_version():
  return get_cluster().hadoop_version


def get_hadoop_config():
  return get_cluster().hadoop_config


def get_build_target_commands():
  return [
    './pants goal bundle mesos:scheduler --bundle-archive=zip',
    './pants goal bundle mesos:executor-%s --bundle-archive=zip' % get_hadoop_version(),
    './pants src/python/twitter/mesos/executor:thermos_executor',
    './pants src/python/twitter/thermos/bin:thermos_run',
    './pants src/python/twitter/mesos:process_scraper',
    './pants src/python/twitter/mesos/executor:gc_executor',
  ]



def get_scheduler_machines():
  cluster = Cluster.get(app.get_options().cluster)

  if app.get_options().really_push:
    params = dict(
      dc = cluster.dc,
      role = cluster.scheduler_role
    )
    if cluster.is_colonized:
      cmd = "colony --server=colony.%(dc)s.twitter.com 'membersOf audubon.role.%(role)s'" % params
    else:
      cmd = 'loony --dc=%(dc)s --group=role:%(role)s --one-column' % params

    result, (output, _) = run_cmd(['ssh', TunnelHelper.get_tunnel_host(cluster.name), cmd])
    if result != 0:
      sys.exit("Failed to determine scheduler hosts for dc: %(dc)s role: %(role)s" % params)
    return [host.strip() for host in output.splitlines()]
  else:
    return ['[dummy-host1]', '[dummy-host2]', '[dummy-host3]']


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
  if app.get_options().verbose or not app.get_options().really_push:
    print '%s command: %s' % (
      'Executing' if app.get_options().really_push else 'Would run', ' '.join(cmd)
    )
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


def fetch_scheduler_http(host, endpoint):
  result = remote_call(host, ['curl', '--silent', '%s/%s' % (SCHEDULER_HTTP, endpoint)])
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


def remote_check_call(host, cmd):
  check_output(['ssh', ssh_target(host)] + cmd)


def check_tag(tag, check_on_master=True):
  """
    Checks that the given tag is valid on origin to enable repeatable builds and returns the sha
    the tag points to.
  """
  repo = Repo()
  if check_on_master:
    if repo.active_branch.name != 'master':
      print >> sys.stderr, 'Deploys must be from master'
      sys.exit(1)

  # TODO(John Sirois): leverage the repo api here where possible
  failed, _ = run_cmd(['git', 'ls-remote', '--exit-code', '--tags', 'origin', tag])
  if failed:
    print >> sys.stderr, 'The tag %s must be on origin' % tag
    sys.exit(1)

  # Find the sha of the commit this heavy-weight tag points to.
  failed, (sha, _) = run_cmd(['git', 'rev-list', '%(tag)s^..%(tag)s' % dict(tag = tag)])
  if failed:
    print >> sys.stderr, 'Failed to find the commit %s points to' % tag
    sys.exit(1)
  tag_sha = sha.splitlines()[0].strip()

  head_sha = repo.head.commit.hexsha
  if head_sha != tag_sha:
    print >> sys.stderr, 'Local repo is not at the expected sha, found %s - please reset' % head_sha
    sys.exit(1)

  return tag_sha


def thermos_postprocess():
  import contextlib
  import zipfile
  with contextlib.closing(zipfile.ZipFile('dist/thermos_executor.pex', 'a')) as zf:
    zf.writestr('twitter/mesos/executor/resources/__init__.py', '')
    zf.write('dist/thermos_run.pex', 'twitter/mesos/executor/resources/thermos_run.pex')


def build():
  for test_cmd in TEST_CMDS:
    print 'Executing test command: %s' % test_cmd
    check_call(test_cmd.split(' '))
  for build_target_cmd in get_build_target_commands():
    print 'Executing build command: %s' % build_target_cmd
    check_call(build_target_cmd.split(' '))
  thermos_postprocess()


def find_current_build(hosts):
  # TODO(John Sirois): consider loony -t
  current_builds = set()
  for host in hosts:
    # the linux machines do not have realpath installed - this is at least portable
    command = [
      'ssh',
      ssh_target(host),
      """"python -c 'import os; print os.path.realpath(\\"%s\\")'" """ % LIVE_BUILD_PATH
    ]
    # TODO(John Sirois): get this to work via remote_call
    result = maybe_run_command(lambda cmd: os.popen(' '.join(cmd)).read(), command)
    if result:
      current_build = result.strip()
      if current_build != LIVE_BUILD_PATH:
        current_builds.add(current_build)

  current_builds = filter(bool, current_builds)
  if (not app.get_options().ignore_conflicting_builds
      and app.get_options().really_push
      and len(current_builds) > 1):
    sys.exit('Found conflicting current builds: %s please resolve manually' % current_builds)
  current_build = current_builds.pop() if current_builds and app.get_options().really_push else None
  print 'Found current build: %s' % current_build
  return current_build


def replace_hdfs_file(host, local_file, hdfs_path):
  BASE_HADOOP_CMD = ['hadoop', '--config', get_hadoop_config(), 'fs']

  remote_call(host, BASE_HADOOP_CMD + ['-mkdir', os.path.dirname(hdfs_path)])
  remote_call(host, BASE_HADOOP_CMD + ['-rm', hdfs_path])
  remote_check_call(host, BASE_HADOOP_CMD + ['-put', local_file, hdfs_path])


def stage_build(hosts):
  result = cmd_output(['bash', '-c',
    'unzip -c %s build.properties | grep build.git.revision' % BUILD_SCHEDULER_JAR_PATH
  ])
  if app.get_options().really_push:
    _, sha = result.split('=')
  else:
    sha = '[sha]'
  release_scheduler_path = '%s/%s-%s' % (RELEASES_DIR, strftime("%Y%m%d%H%M%S", gmtime()), sha)

  print 'Staging the build at: %s on:\n\t%s' % (release_scheduler_path, '\n\t'.join(hosts))

  # Stage release dirs on all hosts
  for host in hosts:
    remote_check_call(host, ['mkdir', '-p', STAGE_DIR])
    check_output(['scp', BUILD_SCHEDULER_PACKAGE_PATH, '%s:%s' % (ssh_target(host),
                                                                  STAGED_PACKAGE_PATH)])
    remote_check_call(host, ['bash', '-c',
      '"mkdir -p %(release_dir)s &&'
      ' unzip -d %(release_dir)s %(staged_package)s &&'
      ' chmod +x %(release_dir)s/scripts/*.sh"' % {
        'release_dir': release_scheduler_path,
        'staged_package': STAGED_PACKAGE_PATH,
      },
    ])

  # Finally stage the HDFS artifacts
  host = hosts[0]
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
    check_output(['scp', local_file, '%s:%s' % (ssh_target(host), stage_file)])
    replace_hdfs_file(host, stage_file, hdfs_target)

  return release_scheduler_path


def set_live_build(hosts, build_path):
  print 'Linking the new build on the scheduler'
  # TODO(John Sirois): consider loony
  for host in hosts:
    remote_check_call(host, ['bash', '-c',
      '"rm -f %(live_build)s &&'
      ' ln -s %(build)s %(live_build)s"' % {
        'live_build': LIVE_BUILD_PATH,
        'build': build_path
      }
    ])


def start_scheduler(hosts):
  print 'Starting the scheduler on %s' % hosts
  # TODO(John Sirois): consider loony
  for host in hosts:
    remote_check_call(host, ['sudo', 'monit', 'start', 'mesos-scheduler'])
  if app.get_options().really_push:
    print 'Waiting for the scheduler to start'
    time.sleep(5)


def scrape_vars(host):
  vars_blob = fetch_scheduler_http(host, 'vars')
  assert vars_blob is not None, 'Failed to fetch vars from scheduler'

  vars = {}
  for kv in (line.split(' ', 1) for line in vars_blob.split('\n')):
    # TODO(John Sirois): yet another sign to support /vars.json export in the science app stack
    k, v = kv if len(kv) == 2 else (kv[0], None)
    vars[k] = v
  return vars


def get_scheduler_uptime_secs(host):
  """Returns the scheduler's uptime in seconds."""

  if not app.get_options().really_push:
    return 0
  return int((scrape_vars(host)).get('jvm_uptime_secs', 0))


def get_scheduler_sha(host):
  """Returns the sha the deployed scheduler is built from."""

  if app.get_options().really_push:
    vars = scrape_vars(host)
    return vars.get('build_git_revision')


def is_scheduler_healthy(host):
  if app.get_options().really_push:
    return fetch_scheduler_http(host, 'health') == 'OK'
  else:
    return True


def stop_scheduler(hosts):
  # TODO(John Sirois): consider loony -t
  for host in hosts:
    print 'Stopping the scheduler'
    print 'Temporarily disabling monit for the scheduler'
    remote_check_call(host, ['sudo', 'monit', 'unmonitor', 'mesos-scheduler'])
    fetch_scheduler_http(host, 'quitquitquit')
    print 'Waiting for scheduler to stop cleanly'
    if app.get_options().really_push:
      time.sleep(5)
    print 'Stopping scheduler via monit'
    remote_check_call(host, ['sudo', 'monit', 'stop', 'mesos-scheduler'])


def watch_scheduler(host, sha, up_min_secs):
  print 'Watching scheduler'
  started = False
  watch_start = time.time()
  start_detected_at = 0
  last_uptime = 0
  # Wait at most three minutes.
  while started or (time.time() - watch_start) < 180:
    if is_scheduler_healthy(host):
      uptime = get_scheduler_uptime_secs(host)
      if not app.get_options().really_push:
        print 'Skipping further health checks, since we are not pushing.'
        return True
      print '%s up and healthy for %s seconds' % (host, uptime)

      if started:
        if uptime < last_uptime:
          print 'Detected scheduler process restart after update (uptime %s)!' % uptime
          return False
        elif (time.time() - start_detected_at) > up_min_secs:
          if sha:
            deployed_sha = get_scheduler_sha(host)
            if deployed_sha != sha:
              print 'Host %s is not on current build %s, has %s' % (host, sha, deployed_sha)
              return False
          print 'Host %s has been up for at least %d seconds' % (host, up_min_secs)
          return True
      else:
        start_detected_at = time.time()

      started = True
      last_uptime = uptime
    elif started:
      print 'Scheduler %s stopped responding to health checks!' % host
      return False
    time.sleep(2)
  return False


def rollback(hosts, rollback_build):
  print 'Initiating rollback'
  set_live_build(hosts, rollback_build)
  start_scheduler(hosts)


app.set_usage('%prog [options] tag')

app.add_option(
  '-v',
  dest='verbose',
  default=False,
  action='store_true',
  help='Verbose logging. (default: %default)')

cluster_list = Cluster.get_list()
app.add_option(
  '--cluster',
  type = 'choice',
  choices = cluster_list,
  dest='cluster',
  help='Cluster to deploy the scheduler in (one of: %s)' % ', '.join(cluster_list))

app.add_option(
  '--skip_build',
  dest='skip_build',
  default=False,
  action='store_true',
  help='Skip build and test, use the existing build. (default: %default)')

app.add_option(
  '--really_push',
  dest='really_push',
  default=False,
  action='store_true',
  help='Safeguard to prevent fat-fingering.  When false, only show commands but do not run them. '
       '(default: %default)')

app.add_option(
  '--ignore_release',
  dest='ignore_release',
  default=False,
  action='store_true',
  help='Ignores the vert release protocol (can only be used in test)')

app.add_option(
  '--hotfix',
  dest='hotfix',
  default=False,
  action='store_true',
  help='Indicates this is a hotfix deploy from a temporary release branch instead of a vert tag')

app.add_option(
  '--ignore_conflicting_builds',
  dest='ignore_conflicting_builds',
  default=False,
  action='store_true',
  help='Ignores conflicting builds')


def main(args, options):
  if not options.really_push:
    print '****************************************************************************************'
    print 'You are running in pretend mode.  None of the commands are actually executed!'
    print 'If you wish to push, add command line arg --really_push'
    print '****************************************************************************************'

  if not options.cluster:
    cluster_list = Cluster.get_list()
    print ('Please specify the cluster you would like to deploy to with\n\t--cluster %s'
           % cluster_list)
    return

  if options.ignore_release and options.cluster == 'smf1-test':
    sha = None
  else:
    if len(args) != 1:
      print 'You must specify the tag you intend to push'
      sys.exit(1)

    sha = check_tag(args[0], check_on_master=not options.hotfix)
    if not sha:
      sys.exit(1)

  if options.skip_build:
    print 'Warning - skipping build, using existing build at %s' % BUILD_SCHEDULER_PACKAGE_PATH
  else:
    build()

  all_schedulers = get_scheduler_machines()

  # Stage the build on all machines and shut all the schedulers down
  current_build = find_current_build(all_schedulers)
  new_build = stage_build(all_schedulers)
  if current_build:
    stop_scheduler(all_schedulers)

  # Point to the new build and start all schedulers up
  # TODO(John Sirois): support a rolling restart once multi-scheduler is used in all enviornments
  set_live_build(all_schedulers, new_build)
  start_scheduler(all_schedulers)

  # TODO(John Sirois): find the leader and health check it for 45 seconds instead
  for scheduler in all_schedulers:
    if not watch_scheduler(scheduler, sha=sha, up_min_secs=15):
      print 'scheduler on %s not healthy' % scheduler
      stop_scheduler(all_schedulers)

      if current_build:
        rollback(all_schedulers, current_build)
        print 'Push rolled back.'
      else:
        print 'Push failed - no previous builds to roll back to.'
      sys.exit(1)

  if options.really_push:
    print 'Push successful!'
  else:
    print 'Fake push completed'

app.main()
