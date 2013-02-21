python_test_suite(name = 'all',
  dependencies = [
    pants(':test_config'),
    pants(':test_updater')
  ],
)

python_tests(name = 'test_config',
  sources = ['test_config.py'],
  dependencies = [
    pants('src/python/twitter/mesos/client:config'),
    python_requirement('mox'),
  ],
)

python_tests(name = 'test_updater',
  sources = ['test_updater.py'],
  dependencies = [
    pants('src/python/twitter/common'),
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)
