python_test_suite(name = 'all',
  dependencies = [
    pants(':cluster_option'),
    pants(':test_http_signaler'),
    pants('tests/python/twitter/mesos/config:all'),
    pants('tests/python/twitter/mesos/client:all'),
    pants('tests/python/twitter/mesos/executor:all'),
    pants('tests/python/twitter/mesos/packer:all'),
    # Commented out till we fix the zookeeper import problem
    #pants('tests/python/twitter/mesos/angrybird:tests'),
  ]
)

python_tests(name = 'cluster_option',
  sources = ['test_cluster_option.py'],
  dependencies = [
    pants('src/python/twitter/mesos:clusters'),
  ]
)

python_tests(name = 'test_http_signaler',
  sources = ['test_http_signaler.py'],
  dependencies = [
    pants('src/python/twitter/mesos/common:http_signaler'),
    python_requirement('mox'),
  ]
)
