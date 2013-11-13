python_binary(
  name = 'thermos_executor',
  source = 'thermos_executor_main.py',
  ignore_errors = True,
  always_write_cache = True,
  dependencies = [
    pants('src/python/twitter/aurora/executor/common:executor_timeout'),
    pants('src/python/twitter/aurora/executor/common:sandbox'),
    pants('src/python/twitter/aurora/executor:executor_detector'),
    pants('src/python/twitter/aurora/executor:executor_vars'),
    pants('src/python/twitter/aurora/executor:thermos_executor'),
    pants('src/python/twitter/aurora/executor:thermos_task_runner'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/metrics'),
  ]
)

python_binary(
  name = 'gc_executor',
  source = 'gc_executor_main.py',
  ignore_errors = True,
  always_write_cache = True,
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/thermos/common'),
    pants('src/python/twitter/aurora/executor:executor_detector'),
    pants('src/python/twitter/aurora/executor:executor_vars'),
    pants('src/python/twitter/aurora/executor:gc_executor'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/metrics'),
  ]
)
