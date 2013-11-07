python_test_suite(name = 'all',
  dependencies = [
    pants(':test_base'),
    pants(':test_constraint_parsing'),
    pants(':test_loader'),
    pants(':test_thrift'),
    pants('tests/python/twitter/aurora/config/schema:all_internal'),
  ]
)

python_tests(name = 'test_base',
  sources = ['test_base.py'],
  dependencies = [
    pants('src/python/twitter/aurora/config'),
    pants('src/python/twitter/common/contextutil'),
  ]
)

python_tests(name = 'test_loader',
  sources = ['test_loader.py'],
  dependencies = [
    pants('src/python/twitter/aurora/config'),
    pants('src/python/twitter/common/contextutil'),
  ],
  coverage = [
    'twitter.aurora.config',
    'twitter.thermos.config'
  ]
)

python_tests(name = 'test_constraint_parsing',
  sources = ['test_constraint_parsing.py'],
  dependencies = [
    pants('src/python/twitter/aurora/config'),
  ]
)

python_tests(name = 'test_thrift',
  sources = ['test_thrift.py'],
  dependencies = [
    pants('src/python/twitter/aurora/config'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift-test'),
  ]
)
