python_library(
  name = 'thermos_task_runner',
  sources = ['thermos_task_runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/twitter/thermos/common'),
    pants('src/main/python/twitter/thermos/config:schema'),
    pants('src/main/python/twitter/thermos/core'),
    pants('src/main/python/twitter/thermos/monitoring:monitor'),
    pants('src/main/python/twitter/aurora/common:http_signaler'),
    pants('src/main/python/twitter/aurora/executor/common:status_checker'),
    pants('src/main/python/twitter/aurora/executor/common:task_info'),
    pants('src/main/python/twitter/aurora/executor/common:task_runner'),
  ]
)

python_library(
  name = 'executor_detector',
  sources = ['executor_detector.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/string'),
  ]
)

python_library(
  name = 'executor_vars',
  sources = ['executor_vars.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:psutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
    pants('aurora/twitterdeps/src/python/twitter/common/python'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('aurora/twitterdeps/src/python/twitter/common/string'),
  ]
)

python_library(
  name = 'status_manager',
  sources = ['status_manager.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/twitter/aurora/executor/common:status_checker'),
  ]
)

python_library(
  name = 'thermos_executor_base',
  sources = ['executor_base.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/thrift/com/twitter/thermos:py-thrift'),
    pants('src/main/python/twitter/aurora:mesos-core'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'thermos_executor',
  sources = ['thermos_executor.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:pystachio'),
    pants(':status_manager'),
    pants(':thermos_executor_base'),
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/concurrent'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/twitter/aurora/executor/common:kill_manager'),
    pants('src/main/python/twitter/aurora/executor/common:sandbox'),
    pants('src/main/python/twitter/aurora/executor/common:task_info'),
    pants('src/main/python/twitter/aurora/executor/common:task_runner'),
    pants('src/main/python/twitter/aurora:mesos-core'),
  ]
)

python_library(
  name = 'thermos_runner',
  sources = ['thermos_runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/twitter/thermos/common:planner'),
    pants('src/main/python/twitter/thermos/config:schema'),
    pants('src/main/python/twitter/thermos/core'),
  ],
)

python_library(
  name = 'gc_executor',
  sources = ['gc_executor.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:psutil'),
    pants(':executor_detector'),
    pants(':thermos_executor_base'),
    pants('aurora/twitterdeps/src/python/twitter/common/collections'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/twitter/thermos/common:ckpt'),
    pants('src/main/python/twitter/thermos/common:path'),
    pants('src/main/python/twitter/thermos/core:helper'),
    pants('src/main/python/twitter/thermos/core:inspector'),
    pants('src/main/python/twitter/thermos/monitoring:detector'),
    pants('src/main/python/twitter/thermos/monitoring:garbage'),
    pants('src/main/python/twitter/aurora/config:schema'),
    pants('src/main/python/twitter/aurora/executor/common:sandbox'),
    pants('src/main/python/twitter/aurora:mesos-core'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'executor-packaged',
  dependencies = [
    # Covering dependencies
    pants('src/main/python/twitter/aurora/common'),
    pants('src/main/python/twitter/aurora/config'),
    pants('src/main/python/twitter/thermos/common'),
    pants('src/main/python/twitter/thermos/config'),
    pants('src/main/python/twitter/thermos/core'),
    pants('src/main/python/twitter/thermos/monitoring'),
  ],
  provides = setup_py(
    name = 'twitter.aurora.executor',
    version = '0.1.2',
  ).with_binaries(
    gc_executor = pants('src/main/python/twitter/aurora/executor/bin:gc_executor'),
    thermos_executor = pants('src/main/python/twitter/aurora/executor/bin:thermos_executor'),
    thermos_runner = pants('src/main/python/twitter/aurora/executor/bin:thermos_runner'),
  )
)
