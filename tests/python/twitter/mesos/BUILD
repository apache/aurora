python_test_suite(name = 'all',
  dependencies = [
    pants(':updater'),
    # TODO(vinod)  Uncomment when Angrybird tests are ready for prime-time.
    # pants('tests/python/twitter/mesos/angrybird:tests'),
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
