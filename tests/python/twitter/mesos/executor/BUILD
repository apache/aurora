python_test_suite(name = 'all',
  dependencies = [
    pants(':sandbox_manager'),
    pants(':gc_executor'),
#   pants(':thermos_executor'),
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

"""
TODO(wickman): This test is currently broken because
test_thermos_executor.py shells out to pants in order to build thermos_run,
but this means that two pants must run in parallel, which no longer works
due to 3a7957f2c2f68c8cd841d69781ab90e35aea6acb.

http://jira.local.twitter.com/browse/MESOS-584

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
"""
