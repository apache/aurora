python_test_suite(name = 'all',
  dependencies = [
    pants(':small'),
  ]
)

python_test_suite(name = 'small',
  dependencies = [
    pants(':test_thermos'),
  ]
)

python_tests(name = 'test_thermos',
  sources = ['test_thermos.py'],
  dependencies = [
     pants('src/main/python/apache/thermos/bin:thermos'),
     pants('src/main/python/apache/thermos/testing:runner'),
  ]
)
