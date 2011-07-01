#!/usr/bin/env python2.6

import sys

class MesosConfiguration:
  def __init__(self, configFile):
    """Loads a job configuration from a file and validates it.

    Returns a validated configuration object.
    """
    self.config = None
    globalScope = {}
    localScope = {}
    execfile(configFile, globalScope, localScope)
    self.config = self.validate(localScope)

  def validate(self, configObj):
    """Validates a configuration object.
    This will make sure that the configuration object has the appropriate
    'magic' fields defined, and are of the correct types.

    We require a job name, and a task definition dictionary
    (which is uninterpreted).  A number of instances may also be specified,
    but will default to 1.

    Returns the job configurations found in the configuration object.
    """

    DEFAULT_BATCH_SIZE = 3
    DEFAULT_RESTART_THRESHOLD = 10
    DEFAULT_WATCH_SECS = 30
    DEFAULT_MAX_PER_SHARD_FAILURE = 0
    DEFAULT_MAX_TOTAL_FAILURE = 0
    if not 'jobs' in configObj:
      print 'Configuration must define a python object named "jobs"'
      sys.exit(2)

    jobs = configObj['jobs']

    if not isinstance(jobs, list):
      print 'Configuration must define a python list named "jobs"'
      sys.exit(2)

    jobDict = {}
    has_errors = False

    for job in jobs:
      errors = []
      if not 'name' in job:
        errors.append('Missing required option: name')
      elif job['name'] in jobDict:
        errors.append('Duplicate job definition')
      if 'owner' in job:
        print >> sys.stderr, "WARNING: 'owner' is deprecated.  Please use role and user."
        if 'role' in job or 'user' in job:
          errors.append('Ambiguous specification: owner and any of role or user')
        else:
          job['role'] = job['owner']
          job['user'] = job['owner']
          del job['owner']
      else:
        if not 'role' in job and not 'user' in job:
          errors.append('Must specify both role and user.')
      if not 'task' in job:
        errors.append('Missing required option: task')
      elif not isinstance(job['task'], dict):
        errors.append('Task configuration must be a python dictionary.')

      if errors:
        has_errors = True
        print 'Invalid configuration!'
        for error in errors:
          print '==> %s' % error
        print '==> %s' % job

      # Default to a single instance.
      if not 'instances' in job:
        job['instances'] = 1

      if not 'cron_schedule' in job:
        job['cron_schedule'] = ''

      if not 'update_config' in job:
        job['update_config'] = {}
      if not 'batchSize' in job['update_config']:
        job['update_config']['batchSize'] = DEFAULT_BATCH_SIZE
      if not 'restartThreshold' in job['update_config']:
        job['update_config']['restartThreshold'] = DEFAULT_RESTART_THRESHOLD
      if not 'watchSecs' in job['update_config']:
        job['update_config']['watchSecs'] = DEFAULT_WATCH_SECS
      if not 'maxPerShardFailures' in job['update_config']:
        job['update_config']['maxPerShardFailures'] = DEFAULT_MAX_PER_SHARD_FAILURE
      if not 'maxTotalFailures' in job['update_config']:
        job['update_config']['maxTotalFailures'] = DEFAULT_MAX_TOTAL_FAILURE

      try:
        jobDict[job['name']] = job
      except KeyError:
        pass

    if has_errors:
      sys.exit(2)
    else:
      return jobDict


if __name__ == '__main__':
  config = MesosConfiguration(sys.argv[1])
  print 'Valid configuration.'
