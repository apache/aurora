python_library(
  name = 'observed_task',
  sources = ['observed_task.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/twitter/thermos:pystachio'),
    pants('src/main/python/twitter/thermos/common:ckpt'),
    pants('src/main/python/twitter/thermos/config'),
  ]
)

python_library(
  name = 'task_observer',
  sources = ['task_observer.py'],
  dependencies = [
    pants(':observed_task'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/twitter/thermos/common:path'),
    pants('src/main/python/twitter/thermos/monitoring:detector'),
    pants('src/main/python/twitter/thermos/monitoring:monitor'),
    pants('src/main/python/twitter/thermos/monitoring:process'),
    pants('src/main/python/twitter/thermos/monitoring:resource'),
    pants('src/main/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'observer',
  dependencies = [
    pants(':task_observer'),
    pants('src/main/python/twitter/thermos/observer/http:http_observer'),

    # covering libraries
    pants('src/main/python/twitter/thermos/common'),
    pants('src/main/python/twitter/thermos/config'),
    pants('src/main/python/twitter/thermos/monitoring'),
  ],
  provides = setup_py(
    name = 'twitter.thermos.observer',
    version = '0.1.0',
    description = 'The Thermos observer web interface.',
  ).with_binaries(
    thermos_observer = pants('src/main/python/twitter/thermos/observer/bin:thermos_observer'),
  )
)
