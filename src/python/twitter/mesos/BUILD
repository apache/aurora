python_library(
  name = 'parsers',
  sources = globs('parsers/*.py'),
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants(':clusters')
  ]
)

python_library(
  name = 'clusters',
  sources = ['clusters.py'],
)

python_library(
  name = 'mesos-lib',
  sources = rglobs('*.py'),
  dependencies = [
    pants('3rdparty/python:zookeeper'),
    pants('3rdparty/python:paramiko'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common_internal/ods'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/mesos/packer'),
    pants('src/thrift/com/twitter/thrift:py-thrift'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_binary(
  name = 'mesos_client',
  source = 'bin/mesos_client.py',
  dependencies = [
    pants(':mesos-lib'),
    pants('src/python/twitter/common'),
    pants('src/python/twitter/thermos/base:options')
  ],
  platforms = ['linux-x86_64'],
)

python_binary(
  name = 'deploy_scheduler',
  source = 'bin/deploy_scheduler.py',
  dependencies = [
    pants(':clusters'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/mesos/deploy'),
  ]
)

python_binary(
  name = 'deploy_thermos',
  source = 'bin/deploy_thermos.py',
  dependencies = [
    pants(':clusters'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/mesos/deploy')
  ]
)

python_binary(
  name = 'restart_job',
  source = 'bin/restart_job.py',
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants(':mesos-lib'),
    pants('3rdparty/python:git-python')
  ]
)

python_binary(
  name = 'thermos_codec',
  source = 'bin/thermos_codec.py',
  dependencies = [
    pants(':mesos-lib'),
    pants('src/python/twitter/common/app'),
  ]
)

python_binary(
  name = 'process_scraper',
  source = 'process_scraper.py',
  dependencies = [
    python_requirement('psutil>=0.5.0'),
    pants('src/python/twitter/common/app'),
  ],
  platforms = ['linux-x86_64'],
)
