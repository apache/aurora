python_test_suite(name = 'all',
  dependencies = [
    pants(':executor_builds'),
    pants(':executor_detector'),
    pants(':executor_vars'),
    pants(':gc_executor'),
    pants(':thermos_executor'),
    pants(':thermos_task_runner'),
    pants('tests/python/twitter/aurora/executor/common:all'),
    pants('tests/python/twitter/aurora/executor/common_internal:all'),
  ]
)

python_tests(name = 'gc_executor',
  sources = ['test_gc_executor.py'],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/core'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/common'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/config'),
    pants('aurora/twitterdeps/src/thrift/com/twitter/thermos:py-thrift'),
    pants('src/python/twitter/aurora/executor:gc_executor'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/concurrent'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/common/testing'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ],
  coverage = 'twitter.mesos.executor.gc_executor'
)


python_tests(name = 'executor_detector',
  sources = [ 'test_executor_detector.py' ],
  dependencies = [
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/aurora/executor:executor_detector')
  ]
)

python_tests(name = 'thermos_executor',
  sources = ['test_thermos_executor.py'],
  timeout = Amount(5, Time.MINUTES),
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/thermos/common'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/core'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/monitoring'),
    pants('src/python/twitter/aurora/config:schema'),
    pants('src/python/twitter/aurora/executor/common:executor_timeout'),
    pants('src/python/twitter/aurora/executor/common:health_checker'),
    pants('src/python/twitter/aurora/executor/common:sandbox'),
    pants('src/python/twitter/aurora/executor/common:task_runner'),
    pants('src/python/twitter/aurora/executor:thermos_executor'),
    pants('src/python/twitter/aurora/executor:thermos_task_runner'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/pants:pants-lib'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'thermos_task_runner',
  sources = ['test_thermos_task_runner.py'],
  dependencies = [
    pants('src/python/twitter/aurora/config:schema'),
    pants('src/python/twitter/aurora/executor/common:sandbox'),
    pants('src/python/twitter/aurora/executor/common:status_checker'),
    pants('src/python/twitter/aurora/executor:thermos_task_runner'),
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
  ]
)

python_tests(name = 'executor_vars',
  sources = ['test_executor_vars.py'],
  dependencies = [
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/aurora/executor:executor_vars'),
  ]
)

python_tests(name = 'executor_builds',
  sources = ['test_executor_builds.py'],
)
