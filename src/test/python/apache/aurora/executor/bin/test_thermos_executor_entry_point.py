def test_thermos_executor_valid_import_dependencies():
  from apache.aurora.executor.bin.thermos_executor_main import proxy_main
  assert proxy_main is not None
