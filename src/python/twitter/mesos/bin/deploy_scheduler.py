import json
import os
import sys
import threading
import time
from time import gmtime, strftime

from twitter.common import app
from twitter.common import log
from twitter.common.dirutil import safe_rmtree
from twitter.common.log import LogOptions
from twitter.common.zookeeper.serverset import ServerSet
from twitter.common_internal.zookeeper.tunneler import TunneledZookeeper
from twitter.mesos.common.cluster_option import ClusterOption
from twitter.mesos.common_internal.cluster_traits import AudubonTrait
from twitter.mesos.common_internal.clusters import TWITTER_CLUSTERS
from twitter.mesos.deploy import Builder, Deployer

from audubondata import Server


class SchedulerManager(object):
  class ConflictingBuilds(Exception): pass

  REMOTE_USER = 'mesos'
  SCHEDULER_HTTP = 'http://localhost:8081'

  MESOS_HOME = '/usr/local/mesos'
  LIVE_BUILD_PATH = '%s/current' % MESOS_HOME
  RELEASES_DIR = '%s/releases' % MESOS_HOME

  STAGE_DIR = '~/release_staging'
  SCHEDULER_PACKAGE = 'mesos-scheduler.zip'
  BUILD_SCHEDULER_PACKAGE_PATH = 'dist/%s' % SCHEDULER_PACKAGE
  BUILD_SCHEDULER_JAR_PATH = 'dist/mesos-scheduler-bundle/mesos-scheduler.jar'
  STAGED_PACKAGE_PATH = '%s/%s' % (STAGE_DIR, SCHEDULER_PACKAGE)

  UPTIME_WAIT_SECS = 180
  LEADER_ELECTION_WAIT_SECS = 300

  def __init__(self, cluster, dry_run=False, verbose=False, ignore_conflicting_schedulers=False):
    self._really_deploy = not dry_run
    self._verbose = verbose
    self._deployer = Deployer(self.REMOTE_USER, dry_run, verbose)
    self._cluster_name = cluster.name
    self._cluster = cluster.with_trait(AudubonTrait)
    self._machines = None
    self._leader = None
    self._ignore_conflicting_schedulers = ignore_conflicting_schedulers

  @property
  def machines(self):
    if not self._machines:
      if not self._really_deploy:
        self._machines = ['[dummy-host1]', '[dummy-host2]', '[dummy-host3]']
      else:
        try:
          machines = Server.match_group('role', value=self._cluster.scheduler_role,
              datacenter=self._cluster.dc)
        except Server.NotFound:
          log.error("Failed to determine scheduler hosts in dc: %s under role: %s" %
              (self._cluster.dc, self._cluster.scheduler_role))
          sys.exit(1)
        self._machines = []
        for machine in machines:
          if not machine.attribute('unmonitored'):
            self._machines.append(machine.hostname)
          else:
            log.info("Ignoring unmonitored host: %s" % machine.hostname)
    return self._machines

  def find_leader(self):
    leader_lock = threading.Event()
    zk = TunneledZookeeper.get(self._cluster.zk, verbose=self._verbose)
    leader = ServerSet(zk, self._cluster.scheduler_zk_path, on_join=lambda _: leader_lock.set())
    log.info("Waiting up to %s seconds for a leader to become elected." % LEADER_ELECTION_WAIT_SECS)
    leader_lock.wait(timeout=LEADER_ELECTION_WAIT_SECS)
    endpoints = list(leader)
    if len(endpoints) == 0:
      log.error("No leading scheduler found!")
      self._leader = None
    else:
      self._leader = endpoints[0].service_endpoint.host
    return self._leader

  def fetch_scheduler_http(self, host, endpoint):
    result = self._deployer.remote_call(host, [
        'curl', '--connect-timeout', '5', '--silent', '%s/%s' % (self.SCHEDULER_HTTP, endpoint)])
    if result is not None:
      return result[1][0].strip()

  def is_scheduler_healthy(self, host):
    if self._really_deploy:
      return self.fetch_scheduler_http(host, 'health') == 'OK'
    else:
      return True

  def _scrape_vars(self, host):
    vars_blob = self.fetch_scheduler_http(host, 'vars.json')
    assert vars_blob is not None, 'Failed to fetch vars from scheduler'
    return json.loads(vars_blob)

  def scheduler_uptime_secs(self, host):
    """Returns the scheduler's uptime in seconds."""
    if not self._really_deploy:
      return 0
    return int((self._scrape_vars(host)).get('jvm_uptime_secs', 0))

  def scheduler_sha(self, host):
    """Returns the sha the deployed scheduler is built from."""
    if self._really_deploy:
      return self._scrape_vars(host).get('build_git_revision')

  def start_scheduler(self, host):
    log.info('Starting the scheduler on %s at %s' % (host, strftime('%Y-%m-%d %H:%M:%S', gmtime())))
    self._deployer.remote_check_call(host, ['sudo', 'monit', 'start', 'mesos-scheduler'])
    if self._really_deploy:
      log.info('Waiting for the scheduler to start')
      time.sleep(5)

  def stop_scheduler(self, host):
    log.info('Stopping the scheduler on %s at %s' % (host, strftime('%Y-%m-%d %H:%M:%S', gmtime())))
    log.info('Temporarily disabling monit for the scheduler on %s' % host)
    self._deployer.remote_check_call(host, ['sudo', 'monit', 'unmonitor', 'mesos-scheduler'])
    self.fetch_scheduler_http(host, 'quitquitquit')
    if self._really_deploy:
      log.info('Waiting for scheduler to stop cleanly')
      time.sleep(5)
    log.info('Stopping scheduler via monit')
    self._deployer.remote_check_call(host, ['sudo', 'monit', 'stop', 'mesos-scheduler'])

  def start_all_schedulers(self):
    log.info('Starting all schedulers: %s' % self.machines)
    # TODO(John Sirois): consider loony
    for host in self.machines:
      self.start_scheduler(host)

  def stop_all_schedulers(self):
    # TODO(John Sirois): consider loony -t
    for host in self.machines:
      self.stop_scheduler(host)

  def find_current_build(self):
    # TODO(John Sirois): consider loony -t
    current_builds = set()
    for host in self.machines:
      # the linux machines do not have realpath installed - this is at least portable
      command = [
        'ssh', self._deployer.ssh_target(host), 'readlink', '-f', self.LIVE_BUILD_PATH
      ]
      # TODO(John Sirois): get this to work via remote_call
      result = self._deployer.maybe_run_command(lambda cmd: os.popen(' '.join(cmd)).read(), command)
      if result:
        current_build = result.strip()
        if current_build != self.LIVE_BUILD_PATH:
          current_builds.add(current_build)

    current_builds = filter(bool, current_builds)
    if not self._ignore_conflicting_schedulers and self._really_deploy and len(current_builds) > 1:
      log.error('Found conflicting current builds: %s please resolve manually' % current_builds)
      raise self.ConflictingBuilds('Found conflicting builds: %s' % current_builds)
    current_build = current_builds.pop() if current_builds and self._really_deploy else None
    log.info('Found current build: %s' % current_build)
    return current_build

  def set_live_build(self, build_path):
    log.info('Linking the new build on the scheduler')
    # TODO(John Sirois): consider loony
    for host in self.machines:
      self._deployer.remote_check_call(host, ['bash', '-c',
        '"rm -f %(live_build)s &&'
        ' ln -s %(build)s %(live_build)s"' % {
          'live_build': self.LIVE_BUILD_PATH,
          'build': build_path
        }
      ])

  def rollback(self, rollback_build=None):
    log.info('Initiating rollback')
    self.stop_all_schedulers()
    if rollback_build:
      self.set_live_build(rollback_build)
      self.start_all_schedulers()

  def stage_build(self, sha):
    release_scheduler_path = '%s/%s-%s' % (
        self.RELEASES_DIR, strftime("%Y%m%d%H%M%S", gmtime()), sha)
    log.info('Staging the build at: %s on:' % release_scheduler_path)
    for host in self.machines:
      log.info('\t%s' % host)

    # Stage release dirs on all hosts
    for host in self.machines:
      self._deployer.remote_check_call(host, ['mkdir', '-p', self.STAGE_DIR])
      self._deployer.check_output(['scp', self.BUILD_SCHEDULER_PACKAGE_PATH, '%s:%s' % (
         self._deployer.ssh_target(host), self.STAGED_PACKAGE_PATH)])
      self._deployer.remote_check_call(host, ['bash', '-c',
        '"mkdir -p %(release_dir)s &&'
        ' unzip -d %(release_dir)s %(staged_package)s &&'
        ' chmod +x %(release_dir)s/scripts/*.sh"' % {
          'release_dir': release_scheduler_path,
          'staged_package': self.STAGED_PACKAGE_PATH,
        },
      ])
    return release_scheduler_path

  def is_up(self, host, sha, minimum_uptime_secs=15):
    log.info('Watching scheduler')
    started = False
    watch_start = time.time()
    start_detected_at = 0
    last_uptime = 0

    # Wait at most three minutes.
    while started or (time.time() - watch_start) < self.UPTIME_WAIT_SECS:
      if self.is_scheduler_healthy(host):
        uptime = self.scheduler_uptime_secs(host)
        if not self._really_deploy:
          log.info('Skipping further health checks, since we are not pushing.')
          return True
        log.info('%s up and healthy for %s seconds at %s' % (host, uptime, strftime('%Y-%m-%d %H:%M:%S', gmtime())))

        if started:
          if uptime < last_uptime:
            log.info('Detected scheduler process restart after update (uptime %s)!' % uptime)
            return False
          elif (time.time() - start_detected_at) > minimum_uptime_secs:
            if sha:
              deployed_sha = self.scheduler_sha(host)
              if deployed_sha != sha:
                log.info('Host %s is not on current build %s, has %s' % (host, sha, deployed_sha))
                return False
            log.info('Host %s has been up for at least %d seconds' % (host, minimum_uptime_secs))
            return True
        else:
          start_detected_at = time.time()

        started = True
        last_uptime = uptime
      elif started:
        log.info('Scheduler %s stopped responding to health checks at %s!' % (host, strftime('%Y-%m-%d %H:%M:%S', gmtime())))
        return False
      time.sleep(2)
    return False


class AuroraBuilder(Builder):
  @property
  def project(self):
    return 'aurora'

  @property
  def test_commands(self):
    return ['./pants goal clean-all test tests/java/com/twitter/mesos:all']

  @property
  def commands(self):
    return ['./pants goal bundle aurora:scheduler --bundle-archive=zip']

  def preprocess(self):
    self.check_call('rm -f pants.pex')



app.set_usage('%prog [options]')

app.add_option('-v', dest='verbose', default=False, action='store_true',
               help='Verbose logging. (default: %default)')

app.add_option(ClusterOption('--cluster', clusters=TWITTER_CLUSTERS))

app.add_option('--skip_build', dest='skip_build', default=False, action='store_true',
               help='Skip build and test, use the existing build.')

app.add_option('--really_push', dest='really_push', default=False, action='store_true',
                help='Safeguard to prevent fat-fingering.  When false, only show commands but do '
                     'not run them.')

app.add_option('--hotfix', dest='hotfix', default=False, action='store_true',
               help='Indicates this is a hotfix deploy from the current tree.')

app.add_option('--ignore_conflicting_builds', dest='ignore_conflicting_builds', default=False,
               action='store_true', help='Ignores conflicting builds')

app.add_option('--release', dest='release', default=None, type=int,
               help='Specify a release number to deploy. If none specified, it uses the latest '
                    'release assigned to the cluster environment.  If specified, it must have '
                    'been assigned as a release for this environment.')

def main(_, options):
  if not options.really_push:
    log.info('****************************************************************************************')
    log.info('You are running in pretend mode.  None of the commands are actually executed!')
    log.info('If you wish to push, add command line arg --really_push')
    log.info('****************************************************************************************')

  dry_run = not options.really_push

  if not options.cluster:
    log.info('Please specify the cluster you would like to deploy to with\n\t--cluster one of %s' %
        ' '.join(TWITTER_CLUSTERS))
    return

  builder = AuroraBuilder(options.cluster, options.release, options.hotfix, options.verbose)

  if not options.skip_build:
    builder.build()

  manager = SchedulerManager(options.cluster, dry_run, options.verbose,
      options.ignore_conflicting_builds)

  current_build = manager.find_current_build()
  new_build = manager.stage_build(builder.sha)
  if current_build:
    manager.stop_all_schedulers()

  manager.set_live_build(new_build)
  manager.start_all_schedulers()

  leading_scheduler = manager.find_leader()
  if not manager.is_up(leading_scheduler, sha=builder.sha):
    log.info('Leading scheduler %s is not healthy' % scheduler)
    manager.rollback(rollback_build=current_build)
    log.info('!!!!!!!!!!!!!!!!!!!!')
    log.info('Release rolled back.')
  else:
    log.info('Push successful!')


LogOptions.disable_disk_logging()
LogOptions.set_stderr_log_level('INFO')
app.main()
