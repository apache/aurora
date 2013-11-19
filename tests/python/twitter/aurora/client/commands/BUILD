python_test_suite(
  name = 'all',
  dependencies = [ pants(':core'), pants(':run'), pants(':ssh') ]
)

python_tests(
  name='core',
  sources = [
    'test_cancel_update.py',
    'test_create.py',
    'test_kill.py',
    'test_listjobs.py',
    'test_restart.py',
    'test_status.py',
    'test_update.py'
  ],
  dependencies = [
    pants('src/python/twitter/aurora/BUILD.thirdparty:mock'),
    pants(':util'),
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('src/python/twitter/aurora/client/commands:core'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ])

python_tests(
  name='ssh',
  sources = [
    'test_ssh.py',
  ],
  dependencies = [
    pants('src/python/twitter/aurora/BUILD.thirdparty:mock'),
    pants(':util'),
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('src/python/twitter/aurora/client/commands:ssh'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ])

python_tests(
  name='run',
  sources = [
    'test_run.py',
  ],
  dependencies = [
    pants('src/python/twitter/aurora/BUILD.thirdparty:mock'),
    pants(':util'),
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('src/python/twitter/aurora/client/commands:run'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ])

python_library(
  name='util',
  sources = [ 'util.py' ],
  dependencies = [
    pants('src/python/twitter/aurora/BUILD.thirdparty:mock'),
    pants('src/python/twitter/aurora/client/commands:core'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
