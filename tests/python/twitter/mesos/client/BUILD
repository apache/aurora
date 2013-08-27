python_test_suite(name = 'all',
  dependencies = [
    pants(':config'),
    pants(':deployment_api'),
    pants(':deployment_cli'),
    pants(':disambiguator'),
    pants(':health_check'),
    pants(':jenkins'),
    pants(':packercli'),
    pants(':quickrun'),
    pants(':restarter'),
    pants(':scheduler_client'),
    pants(':shard_watcher'),
    pants(':updater'),
  ],
)

python_tests(name = 'config',
  sources = ['test_config.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/mesos/client:config'),
    pants('src/python/twitter/mesos/client:jenkins'),
  ],
)

python_tests(name = 'disambiguator',
  sources = ['test_disambiguator.py'],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/mesos/client:disambiguator'),
    pants('3rdparty/python:mox'),
  ],
)

python_tests(name = 'health_check',
  sources = ['test_health_check.py'],
  dependencies = [
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('3rdparty/python:mox'),
  ],
)

python_tests(name = 'packercli',
  sources = [ 'test_packer_cli.py' ],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/mesos:mesos_client_lib'),
  ],
)

python_tests(name = 'quickrun',
  sources = ['test_quickrun.py'],
  dependencies = [
    pants('src/python/twitter/mesos/client:quickrun'),
    pants('3rdparty/python:mox'),
  ]
)

python_tests(name = 'restarter',
  sources = ['test_restarter.py'],
  dependencies = [
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('3rdparty/python:mox'),
  ],
)

python_tests(name = 'scheduler_client',
  sources = ['test_scheduler_client.py'],
  dependencies = [
    pants('src/python/twitter/common'),
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('3rdparty/python:mox'),
  ]
)

python_tests(name = 'shard_watcher',
  sources = ['test_shard_watcher.py'],
  dependencies = [
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('3rdparty/python:mox'),
  ]
)

python_tests(name = 'deployment_api',
  sources = ['test_deployment_api.py'],
  dependencies = [
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/python/twitter/packer'),
    pants('3rdparty/python:mox'),
  ],
)

python_tests(name = 'deployment_cli',
  sources = ['test_deployment_cli.py'],
  dependencies = [
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('3rdparty/python:mox'),
  ],
)

python_tests(name = 'updater',
  sources = ['test_updater.py'],
  dependencies = [
    pants('src/python/twitter/common'),
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('3rdparty/python:mox'),
  ]
)

python_tests(name = 'jenkins',
  sources = ['test_jenkins.py'],
  soft_dependencies = True,
  dependencies = [
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/mesos/client:jenkins'),
    pants('3rdparty/python:dingus'),
  ],
)
