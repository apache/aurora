python_library(
  name = 'observed_task',
  sources = ['observed_task.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos:pystachio'),
    pants('src/python/twitter/thermos/common:ckpt'),
    pants('src/python/twitter/thermos/config'),
  ]
)

python_library(
  name = 'task_observer',
  sources = ['task_observer.py'],
  dependencies = [
    pants(':observed_task'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/thermos/common:path'),
    pants('src/python/twitter/thermos/monitoring:detector'),
    pants('src/python/twitter/thermos/monitoring:monitor'),
    pants('src/python/twitter/thermos/monitoring:process'),
    pants('src/python/twitter/thermos/monitoring:resource'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'observer',
  dependencies = [
    pants(':task_observer'),
    pants('src/python/twitter/thermos/observer/http:http_observer'),

    # covering libraries
    pants('src/python/twitter/thermos/common'),
    pants('src/python/twitter/thermos/config'),
    pants('src/python/twitter/thermos/monitoring'),
  ],
  provides = setup_py(
    name = 'twitter.thermos.observer',
    version = '0.1.0',
    description = 'The Thermos observer web interface.',
  ).with_binaries(
    thermos_observe = pants('src/python/twitter/thermos/observer/bin:thermos_observe'),
  )
)
