python_binary(
  name = 'thermos_observe',
  source = 'thermos_observe.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common/app/modules:http'),
    pants('src/python/twitter/common/app/modules:vars'),
    pants('src/python/twitter/common_internal/app/modules:chickadee_handler'),
    pants('src/python/twitter/common/metrics'),
    pants('src/python/twitter/thermos/observer'),
    pants('src/python/twitter/thermos/monitoring'),
    python_requirement('psutil'),
  ],
)
