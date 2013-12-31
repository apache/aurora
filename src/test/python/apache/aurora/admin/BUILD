python_test_suite(name = 'all',
  dependencies = [
    pants(':mesos_maintenance'),
  ],
)

python_tests(name = 'mesos_maintenance',
  sources = ['test_mesos_maintenance.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/common:cluster'),
    pants('src/main/python/twitter/aurora/admin:mesos_maintenance'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:mock'),
  ],
)
