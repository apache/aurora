python_test_suite(
  name = 'all',
  dependencies = [ pants(':job') ]
)

python_library(
  name = 'util',
  sources = [ 'util.py' ],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:mock'),
    pants('src/main/python/apache/aurora/client/cli'),
  ]
)

python_tests(
  name = 'job',
<<<<<<< HEAD
  sources = [ 'test_create.py', 'test_kill.py', 'test_status.py', 'test_diff.py' ],
=======
  sources = [ 'test_create.py', 'test_kill.py', 'test_status.py' ],
>>>>>>> v2.2
  dependencies = [
    pants(':util'),
    pants('src/main/python/apache/aurora/BUILD.thirdparty:mock'),
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('src/main/python/apache/aurora/client/cli'),
    pants('src/main/python/apache/aurora/client/cli'),
    pants('src/test/python/apache/aurora/client/commands:util')
  ]
)
