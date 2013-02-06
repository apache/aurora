python_test_suite(name = 'all',
  dependencies = [
    pants(':discovery_manager'),
    pants(':gc_executor'),
    pants(':resource_manager'),
    pants(':sandbox_manager'),
    pants(':status_manager'),
    pants(':thermos_executor'),
  ]
)

python_tests(name = 'gc_executor',
  sources = ['test_gc_executor.py'],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/mesos/executor:gc_executor_lib'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ],
  coverage = 'twitter.mesos.executor.gc_executor'
)

python_tests(name = 'resource_manager',
  sources = [ 'test_resource_manager.py' ],
  dependencies = [
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/mesos/executor:resource_manager')
  ]
)

python_tests(name = 'sandbox_manager',
  sources = ['test_sandbox_manager.py'],
  dependencies = [
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/mesos/executor:sandbox_manager'),
  ],
)

python_tests(name = 'discovery_manager',
  sources = ['test_discovery_manager.py'],
  timeout = Amount(5, Time.MINUTES),
  dependencies = [
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/mesos/executor:discovery_manager'),
    pants('src/python/twitter/common/zookeeper:testing'),
    pants('src/python/twitter/common/zookeeper/serverset'),
    pants('src/python/twitter/common_internal/zookeeper:twitter_service')
  ]
)

python_tests(name = 'http_signaler',
  sources = [ 'test_http_signaler.py' ],
  dependencies = [
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/mesos/executor:http_signaler'),
  ]
)

python_tests(name = 'status_manager',
  sources = [ 'test_status_manager.py' ],
  dependencies = [
    python_requirement('tornado'),
    pants('src/python/twitter/common/testing'),
    pants('src/python/twitter/mesos/executor:http_signaler'),
    pants('src/python/twitter/mesos/executor:status_manager')
  ]
)

python_tests(name = 'thermos_executor',
  sources = ['test_thermos_executor.py'],
  timeout = Amount(5, Time.MINUTES),
  dependencies = [
    pants('src/python/twitter/pants:pants-lib'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/thermos/runner'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
    pants('src/python/twitter/mesos/executor:thermos_executor_lib'),
  ]
)

