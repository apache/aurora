python_library(
  name = 'clusters',
  sources = ['clusters.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/options'),
  ]
)

python_library(
  name = 'mesos-core',
  dependencies = [
    python_requirement('mesos==181')
  ]
)

python_library(
  name = 'mesos-lib',
  dependencies = [
    pants(':clusters'),
    pants('src/python/twitter/mesos/client'),
    pants('src/python/twitter/mesos/packer'),
  ]
)

python_binary(
  name = 'mesos_client',
  source = 'bin/mesos_client.py',
  dependencies = [
    python_requirement('argparse'), # this is a legacy dependency required for macaw deploy_mesos
    pants(':mesos-lib'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos/base:options')
  ],
  platforms = (
    'linux-x86_64',
    'macosx-10.6-x86_64',
  )
)

python_binary(
  name = 'mesos_client_full',
  source = 'bin/mesos_client.py',
  dependencies = [
    pants(':mesos-lib'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos/base:options'),
    pants('src/python/twitter/mesos/client:spawn_local'),
  ],
)

python_binary(
  name = 'mesos_admin',
  source = 'bin/mesos_admin.py',
  dependencies = [
    pants(':mesos-lib'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log')
  ],
  platforms = (
    'linux-x86_64',
    'macosx-10.6-x86_64',
  )
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
