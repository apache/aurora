python_test_suite(name = 'all',
  dependencies = [
    pants(':small'),
    pants(':medium'),
    pants(':large'),
  ]
)

python_test_suite(name = 'small',
  dependencies = [
    pants(':test_process'),
  ]
)

python_test_suite(name = 'medium',
  dependencies = [
    pants(':test_ephemerals'),
    pants(':test_failing_runner'),
    pants(':test_failure_limit'),
    pants(':test_finalization'),
    pants(':test_helper'),
    pants(':test_runner_integration'),
 ]
)

python_test_suite(name = 'large',
  dependencies = [
    pants(':test_angry'),
    pants(':test_staged_kill')
  ]
)

python_tests(name = 'test_runner_integration',
  sources = ['test_runner_integration.py'],
  dependencies = [
    pants('src/python/twitter/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_angry',
  sources = ['test_angry.py'],
  dependencies = [
    pants('src/python/twitter/thermos/testing:runner'),
  ],
  timeout = Amount(5, Time.MINUTES)
)

python_tests(name = 'test_ephemerals',
  sources = ['test_ephemerals.py'],
  dependencies = [
    pants('src/python/twitter/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_failing_runner',
  sources = ['test_failing_runner.py'],
  dependencies = [
    pants('src/python/twitter/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_finalization',
  sources = ['test_finalization.py'],
  dependencies = [
    pants('src/python/twitter/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_failure_limit',
  sources = ['test_failure_limit.py'],
  dependencies = [
    pants('src/python/twitter/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_helper',
  sources = ['test_helper.py'],
  dependencies = [
    pants('src/python/twitter/thermos/BUILD.thirdparty:mock'),
    pants('src/python/twitter/thermos/BUILD.thirdparty:psutil'),
    pants('src/python/twitter/thermos/core:helper'),
  ]
)

python_tests(name = 'test_process',
  sources = ['test_process.py'],
  dependencies = [
    pants('src/python/twitter/thermos/BUILD.thirdparty:mock'),
    pants('src/python/twitter/thermos/testing:runner'),
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
  ],
)

python_tests(name = 'test_staged_kill',
  sources = ['test_staged_kill.py'],
  dependencies = [
     pants('src/python/twitter/thermos/testing:runner'),
     pants('aurora/twitterdeps/src/python/twitter/common/process'),
     pants('src/python/twitter/thermos/monitoring:monitor'),
  ]
)
