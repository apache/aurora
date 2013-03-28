python_test_suite(name = 'all',
  dependencies = [
    pants(':mesos_vars'),
  ]
)

python_tests(name = 'mesos_vars',
  sources = ['test_mesos_vars.py'],
  dependencies = [
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/mesos/observer:mesos_vars'),
  ]
)
