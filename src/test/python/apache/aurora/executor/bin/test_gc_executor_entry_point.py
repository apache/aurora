def test_gc_executor_valid_import_dependencies():
  from apache.aurora.executor.bin.gc_executor_main import proxy_main
  assert proxy_main is not None
