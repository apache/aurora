python_test_suite(name = 'all',
  dependencies = [
    pants('tests/python/twitter/mesos/common:all'),
    pants('tests/python/twitter/mesos/common_internal/auth:all'),
    pants('tests/python/twitter/mesos/config:all'),
    pants('tests/python/twitter/mesos/client:all'),
    pants('tests/python/twitter/mesos/executor:all'),
    pants('tests/python/twitter/mesos/observer:all'),
    pants('tests/python/twitter/mesos/tools:all'),
    # Commented out till we fix the zookeeper import problem
    #pants('tests/python/twitter/mesos/angrybird:tests'),
  ]
)
