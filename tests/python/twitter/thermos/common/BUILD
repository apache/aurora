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
    pants('src/python/twitter/thermos/common:path'),
  ],
  coverage = 'twitter.thermos.base.path'
)

python_tests(name = 'test_planner',
  sources = ['test_planner.py'],
  dependencies = [
    pants('src/python/twitter/thermos/common:planner'),
  ],
  coverage = 'twitter.thermos.base.planner'
)

python_tests(name = 'test_task_planner',
  sources = ['test_task_planner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/testing'),
    pants('src/python/twitter/thermos/common:planner'),
    pants('src/python/twitter/thermos/config:schema'),
  ],
)
