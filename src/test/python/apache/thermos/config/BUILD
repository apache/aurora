python_test_suite(name = 'all',
  dependencies = [
    pants(':test_schema'),
  ]
)

python_tests(name = 'test_schema',
  sources = ['test_schema.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/collections'),
    pants('src/main/python/apache/thermos/config:schema')
  ]
)
