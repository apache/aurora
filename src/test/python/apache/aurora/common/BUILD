python_test_suite(
  name = 'all',
  dependencies = [
    pants(':test_aurora_job_key'),
    pants(':test_cluster'),
    pants(':test_clusters'),
    pants(':test_cluster_option'),
    pants(':test_http_signaler'),
  ]
)

python_tests(name = 'test_aurora_job_key',
  sources = ['test_aurora_job_key.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/common:aurora_job_key')
  ],
)

python_tests(
  name = 'test_cluster',
  sources = ['test_cluster.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/common:cluster'),
  ]
)

python_tests(
  name = 'test_clusters',
  sources = ['test_clusters.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:pyyaml'),
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('src/main/python/apache/aurora/common:clusters'),
  ]
)

python_tests(
  name = 'test_cluster_option',
  sources = ['test_cluster_option.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/options'),
    pants('src/main/python/apache/aurora/common:cluster'),
    pants('src/main/python/apache/aurora/common:cluster_option'),
    pants('src/main/python/apache/aurora/common:clusters'),
  ]
)

python_tests(
  name = 'test_http_signaler',
  sources = ['test_http_signaler.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:mox'),
    pants('src/main/python/apache/aurora/common:http_signaler'),
  ]
)
