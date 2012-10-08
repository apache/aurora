python_test_suite(name = 'all',
  dependencies = [
    pants(':client_util')
  ],
)

python_tests(name = 'client_util',
  sources = ['test_client_util.py'],
  soft_dependencies = True,
  dependencies = [
    pants('src/python/twitter/mesos/client'),
    python_requirement('mox'),
  ],
)
