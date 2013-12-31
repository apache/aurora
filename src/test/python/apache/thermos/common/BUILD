python_test_suite(name = 'all',
  dependencies = [
    pants(':test_pathspec'),
    pants(':test_planner'),
    pants(':test_task_planner'),
  ]
)

python_tests(name = 'test_pathspec',
  sources = ['test_pathspec.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/common:path'),
  ],
  coverage = 'apache.thermos.base.path'
)

python_tests(name = 'test_planner',
  sources = ['test_planner.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/common:planner'),
  ],
  coverage = 'apache.thermos.base.planner'
)

python_tests(name = 'test_task_planner',
  sources = ['test_task_planner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/testing'),
    pants('src/main/python/apache/thermos/common:planner'),
    pants('src/main/python/apache/thermos/config:schema'),
  ],
)
