python_test_suite(
  name = 'all',
  dependencies = [
    pants(':health_checker'),
  ]
)

python_tests(
  name = 'health_checker',
  sources = [ 'test_health_checker.py' ],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/aurora/executor/common:health_checker'),
    pants('src/python/twitter/common/testing'),
  ]
)
