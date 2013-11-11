python_test_suite(
  name = 'all',
  dependencies = [ pants(':commands') ]
)

python_tests(
  name='commands',
  sources = [ 'test_create.py', 'test_kill.py', 'test_update.py', 'test_listjobs.py',
      'test_status.py' ],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('src/python/twitter/aurora/client/commands:core'),
    pants('src/python/twitter/common/contextutil'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants(':util')
  ])

python_library(
  name='util',
  sources = [ 'util.py' ],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('src/python/twitter/aurora/client/commands:core'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
