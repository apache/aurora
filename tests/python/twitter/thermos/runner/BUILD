python_test_suite(name = 'all',
  dependencies = [
    pants(':test_ephemerals'),
    pants(':test_failing_runner'),
    pants(':test_failure_limit'),
    pants(':test_planner'),
    pants(':test_runner_integration'),
    pants(':test_task_planner'),
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

python_tests(name = 'test_failing_runner',
  sources = ['test_failing_runner.py'],
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

python_tests(name = 'test_planner',
  sources = ['test_planner.py'],
  dependencies = [
    pants('src/python/twitter/thermos/runner'),
  ]
)

python_tests(name = 'test_task_planner',
  sources = ['test_task_planner.py'],
  dependencies = [
    pants('src/python/twitter/common/testing'),
    pants('src/python/twitter/thermos/runner'),
  ]
)
