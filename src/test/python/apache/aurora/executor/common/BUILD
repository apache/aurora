python_test_suite(
  name = 'all',
  dependencies = [
    pants(':directory_sandbox'),
    pants(':health_checker'),
    pants(':status_checker'),
    pants(':task_info'),
  ]
)

python_tests(
  name = 'directory_sandbox',
  sources = ['test_directory_sandbox.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:mock'),
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('src/main/python/apache/aurora/executor/common:sandbox'),
  ],
)

python_tests(
  name = 'health_checker',
  sources = ['test_health_checker.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:mox'),
    pants('aurora/twitterdeps/src/python/twitter/common/testing'),
    pants('src/main/python/apache/aurora/executor/common:health_checker'),
  ]
)

python_tests(
  name = 'status_checker',
  sources = ['test_status_checker.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/executor/common:status_checker'),
  ]
)

python_tests(
  name = 'task_info',
  sources = ['test_task_info.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/executor/common:task_info'),
    pants('src/main/python/apache/aurora/config/schema'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)
