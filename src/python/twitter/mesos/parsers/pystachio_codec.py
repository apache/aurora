from pystachio import Map, String

from twitter.mesos.clusters import Cluster
from twitter.mesos.config.schema import (
  MesosJob,
  UpdateConfig,
)
from twitter.thermos.config.schema import (
  Process,
  Resources,
  Task,
)

from .base import EntityParser
from .mesos_config import MesosConfig
from .pystachio_thrift import convert as convert_pystachio_to_thrift


class PystachioCodec(MesosConfig):
  def __init__(self, filename, name=None):
    super(PystachioCodec, self).__init__(filename, name)

  def build_constraints(self):
    return Map(String, String)(self._config.get('constraints', {}))

  def build_update_config(self):
    return UpdateConfig(self.get_update_config(self._config, snaked=True))

  def build_task(self):
    cfg = self._config
    processes = []
    if self.hdfs_path():
      processes.append(Process(
          name = 'installer',
          cmdline = 'hadoop --config {{hadoop.config}} fs -copyToLocal %s .' % self.hdfs_path(),
          max_failures = 5))
    cmdline = cfg['task']['start_command']
    # rewrite ports in cmdline
    for port in EntityParser.match_ports(cmdline):
      cmdline = cmdline.replace('%port:{0}%'.format(port), '{{thermos.ports[%s]}}' % port)
    cmdline = cmdline.replace('%shard_id%', '{{mesos.instance}}')
    cmdline = cmdline.replace('%task_id%', '{{thermos.task_id}}')
    main_process = Process(
      name = self.name(),
      cmdline = cmdline
    )
    processes.append(main_process)
    task_dict = dict(
      name = self.name(),
      processes = processes,

      resources = Resources(
        cpu  = cfg['task']['num_cpus'],
        ram  = cfg['task']['ram_mb'] * 1048576,
        disk = cfg['task']['disk_mb'] * 1048576),
    )
    if len(processes) > 1:
      task_dict.update(constraints = [{'order': [process.name() for process in processes]}])
    return Task(task_dict)

  def build(self):
    """Returns a configuration in the MesosJob pystachio schema."""
    cfg = self._config
    job_dict = dict(
      name = self.name(),
      role = cfg['role'],
      instances = cfg['instances'],
      task = self.build_task(),
      cron_schedule = cfg['cron_schedule'],
      cron_policy = cfg['cron_collision_policy'],
      update_config = self.build_update_config(),
      constraints = self.build_constraints(),
      daemon = cfg['task']['daemon'],
      max_task_failures = cfg['task']['max_task_failures'],
      production = cfg['task']['production'],
      priority = cfg['task']['priority'])
    if cfg['task'].get('health_check_interval_secs'):
      job_dict['health_check_interval_secs'] = cfg['task']['health_check_interval_secs']
    return MesosJob(job_dict) % Cluster.get(cfg['cluster']).context()

  def job(self):
    return convert_pystachio_to_thrift(self.build())
