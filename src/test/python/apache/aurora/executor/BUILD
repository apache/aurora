# TODO(jfarrell): AURORA-14 for executor_builds, gc_executor and thermos_executor
python_test_suite(name = 'all',
  dependencies = [
    # pants(':executor_builds'),
    pants(':executor_detector'),
    pants(':executor_vars'),
    # pants(':gc_executor'),
    # pants(':thermos_executor'),
    pants(':thermos_task_runner'),
    pants('src/test/python/apache/aurora/executor/common:all'),
  ]
)

python_tests(name = 'gc_executor',
  sources = ['test_gc_executor.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:mock'),
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/concurrent'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('aurora/twitterdeps/src/python/twitter/common/testing'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/python/apache/thermos/config'),
    pants('src/main/python/apache/thermos/core:runner'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
    pants('src/main/python/apache/aurora/executor:gc_executor'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ],
  coverage = 'twitter.mesos.executor.gc_executor'
)

python_tests(name = 'executor_detector',
  sources = [ 'test_executor_detector.py' ],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('src/main/python/apache/aurora/executor:executor_detector')
  ]
)

python_tests(name = 'thermos_executor',
  sources = ['test_thermos_executor.py'],
  timeout = Amount(5, Time.MINUTES),
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/apache/thermos/common'),
    pants('src/main/python/apache/thermos/core'),
    pants('src/main/python/apache/thermos/monitoring:monitor'),
    pants('src/main/python/apache/aurora/config:schema'),
    pants('src/main/python/apache/aurora/executor/common:executor_timeout'),
    pants('src/main/python/apache/aurora/executor/common:health_checker'),
    pants('src/main/python/apache/aurora/executor/common:sandbox'),
    pants('src/main/python/apache/aurora/executor/common:task_runner'),
    pants('src/main/python/apache/aurora/executor:thermos_executor'),
    pants('src/main/python/apache/aurora/executor:thermos_task_runner'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'thermos_task_runner',
  sources = ['test_thermos_task_runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/apache/aurora/config:schema'),
    pants('src/main/python/apache/aurora/executor/common:sandbox'),
    pants('src/main/python/apache/aurora/executor/common:status_checker'),
    pants('src/main/python/apache/aurora/executor:thermos_task_runner'),
  ]
)

python_tests(name = 'executor_vars',
  sources = ['test_executor_vars.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('src/main/python/apache/aurora/executor:executor_vars'),
  ]
)

python_tests(name = 'executor_builds',
  sources = ['test_executor_builds.py'],
)
