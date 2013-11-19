python_binary(
  name = 'thermos_executor',
  source = 'thermos_executor_main.py',
  entry_point = 'twitter.aurora.executor.bin.thermos_executor_main:proxy_main',
  ignore_errors = True,
  always_write_cache = True,
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
    pants('src/python/twitter/aurora/executor/common:executor_timeout'),
    pants('src/python/twitter/aurora/executor/common:health_checker'),
    pants('src/python/twitter/aurora/executor/common:sandbox'),
    pants('src/python/twitter/aurora/executor:executor_detector'),
    pants('src/python/twitter/aurora/executor:executor_vars'),
    pants('src/python/twitter/aurora/executor:thermos_executor'),
    pants('src/python/twitter/aurora/executor:thermos_task_runner'),
  ]
)

python_binary(
  name = 'gc_executor',
  source = 'gc_executor_main.py',
  entry_point = 'twitter.aurora.executor.bin.gc_executor_main:proxy_main',
  ignore_errors = True,
  always_write_cache = True,
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
    pants('src/python/twitter/thermos/common:path'),
    pants('src/python/twitter/aurora/executor:executor_detector'),
    pants('src/python/twitter/aurora/executor:executor_vars'),
    pants('src/python/twitter/aurora/executor:gc_executor'),
  ]
)

python_binary(
  name = 'thermos_runner',
  source = 'thermos_runner_main.py',
  entry_point = 'twitter.aurora.executor.bin.thermos_runner_main:proxy_main',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/python/twitter/aurora/executor:thermos_runner'),
  ],
)
