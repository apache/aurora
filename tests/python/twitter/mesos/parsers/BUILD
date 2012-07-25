python_test_suite(name = 'all',
  dependencies = [
    pants(':test_mesos_config'),
    pants(':test_mesos_to_pystachio'),
    pants(':test_pystachio_to_thrift'),
    pants(':test_pystachio_config'),
  ]
)

python_tests(name = 'test_mesos_config',
  sources = ['test_mesos_config.py'],
  dependencies = [
    pants('src/python/twitter/mesos:parsers'),
  ]
)

python_tests(name = 'test_mesos_to_pystachio',
  sources = ['test_mesos_to_pystachio.py'],
  dependencies = [
    pants('src/python/twitter/mesos:parsers'),
  ]
)

python_tests(name = 'test_pystachio_to_thrift',
  sources = ['test_pystachio_to_thrift.py'],
  dependencies = [
    pants('src/python/twitter/mesos:parsers'),
  ]
)

python_tests(name = 'test_pystachio_config',
  sources = ['test_pystachio_config.py'],
  dependencies = [
    pants('src/python/twitter/mesos:parsers'),
  ]
)
