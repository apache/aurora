python_test_suite(name = 'all',
  dependencies = [
    pants(':dangerous_rewrite_shards'),
  ],
)

python_tests(name = 'dangerous_rewrite_shards',
  sources = ['test_dangerous_rewrite_shards.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/mesos/client:api'),
    pants('src/python/twitter/mesos/tools:dangerous_rewrite_shards_lib'),
  ],
)
