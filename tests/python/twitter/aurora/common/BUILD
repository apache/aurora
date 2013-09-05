python_test_suite(
  name = 'all',
  dependencies = [
    pants(':test_aurora_job_key'),
    pants(':test_cluster'),
    pants(':test_cluster_option'),
    pants(':test_http_signaler'),
  ]
)

python_tests(name = 'test_aurora_job_key',
  sources = ['test_aurora_job_key.py'],
  dependencies = [
    pants('src/python/twitter/aurora/common:aurora_job_key')
  ],
)

python_tests(
  name = 'test_cluster',
  sources = ['test_cluster.py'],
  dependencies = [
    pants('src/python/twitter/aurora/common:cluster'),
  ]
)

python_tests(
  name = 'test_cluster_option',
  sources = ['test_cluster_option.py'],
  dependencies = [
    pants('src/python/twitter/aurora/common:cluster'),
    pants('src/python/twitter/aurora/common:clusters'),
    pants('src/python/twitter/aurora/common:cluster_option'),
    pants('src/python/twitter/common/options'),
  ]
)

python_tests(
  name = 'test_http_signaler',
  sources = ['test_http_signaler.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/aurora/common:http_signaler'),
  ]
)
