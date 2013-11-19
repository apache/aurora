python_test_suite(name = 'all',
  dependencies = [
    pants(':test_disk'),
  ]
)

python_tests(name = 'test_disk',
  sources = ['test_disk.py'],
  dependencies = [
    pants('src/main/python/twitter/thermos/monitoring:disk'),
  ]
)

