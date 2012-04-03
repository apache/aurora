python_library(
  name = 'sandbox_manager',
  sources = ['sandbox_manager.py'],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common_internal/appapp')
  ]
)

python_library(
  name = 'task_runner_wrapper',
  sources = ['task_runner_wrapper.py'],
  dependencies = [
    pants(':sandbox_manager'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/http'),
    pants('src/python/twitter/thermos/base'),
    pants('src/python/twitter/thermos/runner'),
    pants('src/python/twitter/thermos/monitoring:monitor'),
    pants('src/python/twitter/thermos/config:schema'),
  ]
)

python_library(
  name = 'thermos_executor_base',
  sources = ['executor_base.py'],
  dependencies = [
    pants('3rdparty/python.twitter:mesos-latest'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common/app/modules:scribe_exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'thermos_executor_lib',
  sources = ['thermos_executor.py'],
  dependencies = [
    pants(':task_runner_wrapper'),
    pants(':thermos_executor_base'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/mesos/config:schema'),
  ]
)

python_binary(
  name = 'thermos_executor',
  source = 'thermos_executor.py',
  ignore_errors = True,
  platforms = [
    'linux-x86_64',
    'macosx-10.4-x86_64',
  ],
  dependencies = [
    pants(':thermos_executor_lib')
  ]
)

python_library(
  name = 'gc_executor_lib',
  sources = ['gc_executor.py'],
  dependencies = [
    pants(':sandbox_manager'),
    pants(':thermos_executor_base'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/thermos/runner'),
    pants('src/python/twitter/thermos/monitoring:detector'),
    pants('src/python/twitter/thermos/monitoring:garbage'),
  ]
)

# This is sort of annoying.
# TODO(wickman) python_binary targets should probably also include their
# un-relocated source.
python_binary(
  name = 'gc_executor',
  source = 'gc_executor.py',
  ignore_errors = True,
  platforms = [
    'linux-x86_64',
    'macosx-10.4-x86_64',
  ],
  dependencies = [
    pants(':gc_executor_lib')
  ]
)
