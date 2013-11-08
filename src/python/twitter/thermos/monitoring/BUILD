python_library(
  name = 'detector',
  sources = ['detector.py'],
  dependencies = [
    pants('src/python/twitter/thermos/common:path')
  ]
)

python_library(
  name = 'garbage',
  sources = ['garbage.py'],
  dependencies = [
    pants(':detector'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/thermos/common:ckpt'),
    pants('src/python/twitter/thermos/common:path'),
  ]
)

python_library(
  name = 'monitor',
  sources = ['monitor.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/python/twitter/thermos/common:ckpt'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'disk',
  sources = ['disk.py'],
  dependencies = [
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    python_requirement('watchdog'),
  ]
)

python_library(
  name = 'process',
  sources = ['process.py'],
)

python_library(
  name = 'process_collector_psutil',
  sources = ['process_collector_psutil.py'],
  dependencies = [
    pants(':process'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos:psutil'),
  ]
)

python_library(
  name = 'resource',
  sources = ['resource.py'],
  dependencies = [
    pants(':disk'),
    pants(':monitor'),
    pants(':process'),
    pants(':process_collector_psutil'),
    pants('src/python/twitter/common/collections'),
    pants('src/python/twitter/common/concurrent'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
  ]
)

python_library(
  name = 'monitoring',
  dependencies = [
    pants(':detector'),
    pants(':disk'),
    pants(':garbage'),
    pants(':monitor'),
    pants(':process'),
    pants(':resource')
  ],
  provides = setup_py(
    name = 'twitter.thermos.monitoring',
    version = '0.1.0',
    description = 'Thermos monitoring library.',
  )
)
