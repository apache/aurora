from pystachio import Map, String

from twitter.mesos.config.schema import (
  MesosJob,
  PackerPackage,
  UpdateConfig,
)
from twitter.thermos.config.loader import ThermosTaskValidator
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

  def build_package(self, package_tuple):
    return PackerPackage(
      role = package_tuple[0],
      name = package_tuple[1],
      version = str(package_tuple[2]))

  def build_task(self):
    cfg = self._config
    processes = []
    installer_command = Process(name = 'installer', max_failures = 5)
    if self.hdfs_path():
      processes.append(installer_command(
          cmdline = 'hadoop fs -copyToLocal %s .' % self.hdfs_path()))
    elif self.package():
      processes.append(installer_command(
          cmdline = '{{packer[%s][%s][%s].copy_command}}' % self.package()))
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
      cluster = cfg['cluster'],
      instances = cfg['instances'],
      task = self.build_task(),
      cron_schedule = cfg['cron_schedule'],
      cron_policy = cfg['cron_collision_policy'],
      update_config = self.build_update_config(),
      constraints = self.build_constraints(),
      daemon = cfg['task']['daemon'],
      max_task_failures = cfg['task']['max_task_failures'],
      production = cfg['task']['production'],
      priority = cfg['task']['priority'],
      task_links = cfg.get('task_links', {}),)
    if cfg['task'].get('health_check_interval_secs'):
      job_dict['health_check_interval_secs'] = cfg['task']['health_check_interval_secs']
    return MesosJob(job_dict)

  def job(self):
    return convert_pystachio_to_thrift(self.build())
