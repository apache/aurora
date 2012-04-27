python_test_suite(name = 'all',
  dependencies = [
    pants(':sandbox_manager'),
    pants(':gc_executor'),
    pants(':thermos_executor'),
    pants(':status_manager'),
  ]
)

python_tests(name = 'sandbox_manager',
  sources = ['test_sandbox_manager.py'],
  soft_dependencies = True,
  dependencies = [
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/mesos/executor:sandbox_manager'),
  ]
)

python_tests(name = 'status_manager',
  sources = [ 'test_status_manager.py' ],
  soft_dependencies = True,
  dependencies = [
    python_requirement('tornado'),
    pants('src/python/twitter/common/testing'),
    pants('src/python/twitter/mesos/executor:status_manager')
  ]
)

python_tests(name = 'gc_executor',
  sources = ['test_gc_executor.py'],
  soft_dependencies = True,
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/mesos/executor:gc_executor_lib'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ]
)

python_tests(name = 'thermos_executor',
  sources = ['test_thermos_executor.py'],
  soft_dependencies = True,
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
