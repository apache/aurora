python_test_suite(
  name = 'all',
  dependencies = [
    ':gc_executor_entry_point',
    ':thermos_executor_entry_point',
  ]
)

python_tests(
  name = 'gc_executor_entry_point',
  sources = ['test_gc_executor_entry_point.py'],
  dependencies = [
    'src/main/python/apache/aurora/executor/bin:gc_executor_source',
  ],
)

python_tests(
  name = 'thermos_executor_entry_point',
  sources = ['test_thermos_executor_entry_point.py'],
  dependencies = [
    'src/main/python/apache/aurora/executor/bin:thermos_executor_source',
  ],
)
