python_test_suite(name = 'all',
  dependencies = [
    pants(':updater'),
    # Commented out till we fix the zookeeper import problem
    #pants('tests/python/twitter/mesos/angrybird:tests'),
  ]
)

python_tests(name = 'updater',
  sources = ['test_updater.py'],
  dependencies = [
    pants('src/python/twitter/common'),
    pants('src/python/twitter/mesos:mesos-lib'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)
