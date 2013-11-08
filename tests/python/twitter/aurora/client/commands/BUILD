python_test_suite(
  name = 'all',
  dependencies = [ pants(':commands') ]
)

python_tests(
  name='commands',
  sources = [ 'test_create.py', 'test_kill.py' ],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('src/python/twitter/aurora/client/commands:core'),
    pants('src/python/twitter/common/contextutil'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ])
