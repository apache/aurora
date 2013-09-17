python_test_suite(name = 'all',
  dependencies = [
    pants(':disambiguator'),
    pants(':job_monitor'),
    pants(':restarter'),
    pants(':scheduler_client'),
    pants(':shard_watcher'),
    pants(':updater'),
  ],
)

python_tests(
  name = 'disambiguator',
  sources = ['test_disambiguator.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/aurora/client/api:disambiguator'),
    pants('src/python/twitter/common/app'),
  ],
)

python_tests(name = 'job_monitor',
  sources = ['test_job_monitor.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/aurora/client/api:api'),
    pants('src/python/twitter/aurora/client/api:job_monitor'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('tests/python/twitter/aurora/client:fake_scheduler_proxy'),
  ],
)

python_tests(name = 'restarter',
  sources = ['test_restarter.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/aurora/client/api:restarter'),
    pants('src/python/twitter/aurora/common:aurora_job_key'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('tests/python/twitter/aurora/client:fake_scheduler_proxy'),
  ],
)

python_tests(name = 'scheduler_client',
  sources = ['test_scheduler_client.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/aurora/client/api:scheduler_client'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'shard_watcher',
  sources = ['test_shard_watcher.py', 'test_health_check.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/aurora/client/api:shard_watcher'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'updater',
  sources = ['test_updater.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/aurora/client/api:updater'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('tests/python/twitter/aurora/client:fake_scheduler_proxy'),
  ]
)
