import os

python_library(
  name = 'detector',
  sources = ['detector.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/common:path')
  ]
)

python_library(
  name = 'garbage',
  sources = ['garbage.py'],
  dependencies = [
    pants(':detector'),
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/common:path'),
  ]
)

python_library(
  name = 'monitor',
  sources = ['monitor.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'disk',
  sources = ['disk.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
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
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/apache/thermos:psutil'),
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
    pants('aurora/twitterdeps/src/python/twitter/common/collections'),
    pants('aurora/twitterdeps/src/python/twitter/common/concurrent'),
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
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
    pants(':resource'),

    # covering dependency for common
    pants('src/main/python/apache/thermos/common'),
  ],
  provides = setup_py(
    name = 'apache.thermos.monitoring',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().lower(),
    description = 'Thermos monitoring library.',
  )
)
