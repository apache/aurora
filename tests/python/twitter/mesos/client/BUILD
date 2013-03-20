python_test_suite(name = 'all',
  dependencies = [
    pants(':config'),
    pants(':scheduler_client'),
    pants(':updater'),
    pants(':shard_watcher'),
  ],
)

python_tests(name = 'config',
  sources = ['test_config.py'],
  dependencies = [
    pants('src/python/twitter/mesos/client:config'),
    python_requirement('mox'),
  ],
)

python_tests(name = 'scheduler_client',
  sources = ['test_scheduler_client.py'],
  dependencies = [
    pants('src/python/twitter/common'),
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    python_requirement('mox'),
  ],
)

python_tests(name = 'updater',
  sources = ['test_updater.py'],
  dependencies = [
    pants('src/python/twitter/common'),
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    python_requirement('mox'),
  ]
)

python_tests(name = 'shard_watcher',
  sources = ['test_shard_watcher.py'],
  dependencies = [
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/python/twitter/mesos/common:http_signaler'),
    python_requirement('mox'),
  ]
)
