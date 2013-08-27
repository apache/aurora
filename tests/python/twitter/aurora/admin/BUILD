python_test_suite(name = 'all',
  dependencies = [
    pants(':mesos_maintenance'),
  ],
)

python_tests(name = 'mesos_maintenance',
  sources = ['test_mesos_maintenance.py'],
  dependencies = [
    pants('src/python/twitter/aurora/admin:mesos_maintenance'),
    pants('src/python/twitter/aurora/common_internal:clusters'),
    pants('3rdparty/python:mox'),
  ],
)
