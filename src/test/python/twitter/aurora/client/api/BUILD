python_test_suite(name = 'all',
  dependencies = [
    pants(':disambiguator'),
    pants(':job_monitor'),
    pants(':restarter'),
    pants(':scheduler_client'),
    pants(':instance_watcher'),
    pants(':updater'),
  ],
)

python_tests(
  name = 'disambiguator',
  sources = ['test_disambiguator.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:mox'),
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('src/main/python/twitter/aurora/client/api:disambiguator'),
  ],
)

python_tests(name = 'job_monitor',
  sources = ['test_job_monitor.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:mox'),
    pants('src/main/python/twitter/aurora/client/api:api'),
    pants('src/main/python/twitter/aurora/client/api:job_monitor'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('src/test/python/twitter/aurora/client:fake_scheduler_proxy'),
  ],
)

python_tests(name = 'restarter',
  sources = ['test_restarter.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:mox'),
    pants('src/main/python/twitter/aurora/client/api:restarter'),
    pants('src/main/python/twitter/aurora/common:aurora_job_key'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('src/test/python/twitter/aurora/client:fake_scheduler_proxy'),
  ],
)

python_tests(name = 'scheduler_client',
  sources = ['test_scheduler_client.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:mox'),
    pants('src/main/python/twitter/aurora/client/api:scheduler_client'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'instance_watcher',
  sources = ['test_instance_watcher.py', 'test_health_check.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:mox'),
    pants('src/main/python/twitter/aurora/client/api:instance_watcher'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'updater',
  sources = ['test_updater.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:mox'),
    pants('src/main/python/twitter/aurora/client/api:updater'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('src/test/python/twitter/aurora/client:fake_scheduler_proxy'),
  ]
)
