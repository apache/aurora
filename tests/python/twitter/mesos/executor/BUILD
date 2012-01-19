python_test_suite(name = 'all',
  dependencies = [
    pants(':sandbox_manager'),
  ]
)

python_tests(name = 'sandbox_manager',
  sources = ['test_sandbox_manager.py'],
  dependencies = [
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/mesos/executor:sandbox_manager'),
  ]
)
