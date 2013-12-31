python_library(
  name = 'status_checker',
  sources = ['status_checker.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
  ]
)

python_library(
  name = 'task_runner',
  sources = ['task_runner.py'],
  dependencies = [
    pants(':status_checker'),
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
  ]
)

python_library(
  name = 'health_checker',
  sources = ['health_checker.py'],
  dependencies = [
    pants(':status_checker'),
    pants(':task_info'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('src/main/python/apache/aurora/common:http_signaler'),
  ]
)

python_library(
  name = 'executor_timeout',
  sources = ['executor_timeout.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
  ]
)

python_library(
  name = 'kill_manager',
  sources = ['kill_manager.py'],
  dependencies = [
    pants(':status_checker'),
  ]
)

python_library(
  name = 'sandbox',
  sources = ['sandbox.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
  ]
)

python_library(
  name = 'task_info',
  sources = ['task_info.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:pystachio'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/apache/aurora/config'),
    pants('src/main/python/apache/aurora/config/schema'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)
