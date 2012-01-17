python_test_suite(name = 'all',
  dependencies = [
    pants(':macro_runner'),
    pants(':failing_macro_runner'),
  ]
)

python_library(name = 'runner_baselibs',
  dependencies = [
    pants('src/python/twitter/thermos:runner'),
  ]
)

python_tests(name = 'macro_runner',
  sources = ['macro_runner_test.py'],
  dependencies = [pants(':runner_baselibs')]
)

python_tests(name = 'failing_macro_runner',
  sources = ['failing_runner_test.py'],
  dependencies = [pants(':runner_baselibs')]
)
