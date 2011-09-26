python_binary(
  name = 'thermos_executor',
  source = 'thermos_executor.py',
  dependencies = [
    pants('src/python/twitter/tcl:loader'),
    pants('src/python/twitter/thermos:base'),
    pants('src/python/twitter/thermos:runner'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('3rdparty/python.twitter:mesos-68')
  ]
)

python_binary(
  name = 'thermos_observing_executor',
  source = 'thermos_observing_executor.py',
  dependencies = [
    pants('src/python/twitter/tcl:loader'),
    pants('src/python/twitter/thermos:base'),
    pants('src/python/twitter/thermos:observer')
  ]
)

python_binary(
  name = 'thermos_test_framework',
  source = 'thermos_test_framework.py',
  dependencies = [
    pants('src/python/twitter/tcl:loader'),
    pants('src/python/twitter/thermos:base'),
    pants('src/python/twitter/thermos:runner')
  ]
)
