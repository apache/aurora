python_test_suite(name = 'all',
  dependencies = [
    pants(':small'),
    pants(':medium'),
    pants(':large'),
  ]
)

python_test_suite(name = 'small',
  dependencies = [
    pants(':test_process'),
  ]
)

python_test_suite(name = 'medium',
  dependencies = [
    pants(':test_ephemerals'),
    pants(':test_failing_runner'),
    pants(':test_failure_limit'),
    pants(':test_finalization'),
    pants(':test_runner_integration'),
 ]
)

python_test_suite(name = 'large',
  dependencies = [
    pants(':test_angry'),
    pants(':test_staged_kill')
  ]
)

python_library(name = 'runner_baselibs',
  dependencies = [
    pants('src/python/twitter/thermos/runner'),
  ]
)

python_tests(name = 'test_runner_integration',
  sources = ['test_runner_integration.py'],
  dependencies = [pants(':runner_baselibs')]
)

python_tests(name = 'test_angry',
  sources = ['test_angry.py'],
  dependencies = [pants(':runner_baselibs')],
  timeout = Amount(5, Time.MINUTES)
)

python_tests(name = 'test_failing_runner',
  sources = ['test_failing_runner.py'],
  dependencies = [pants(':runner_baselibs')]
)

python_tests(name = 'test_finalization',
  sources = ['test_finalization.py'],
  dependencies = [pants(':runner_baselibs')]
)

python_tests(name = 'test_ephemerals',
  sources = ['test_ephemerals.py'],
  dependencies = [pants(':runner_baselibs')]
)

python_tests(name = 'test_failure_limit',
  sources = ['test_failure_limit.py'],
  dependencies = [pants(':runner_baselibs')]
)

python_tests(name = 'test_process',
  sources = ['test_process.py'],
  dependencies = [
    pants(':runner_baselibs'),
    pants('3rdparty/python:mock')
  ],
)

python_tests(name = 'test_staged_kill',
  sources = ['test_staged_kill.py'],
  dependencies = [
     pants(':runner_baselibs'),
     pants('src/python/twitter/thermos/monitoring:monitor'),
  ]
)
