python_test_suite(name = 'all',
  dependencies = [
    pants(':aurora_job_key'),
    pants(':http_signaler'),
  ],
)

python_tests(name = 'aurora_job_key',
  sources = ['test_aurora_job_key.py'],
  dependencies = [
    pants('src/python/twitter/mesos/common:aurora_job_key')
  ],
)

python_tests(name = 'http_signaler',
  sources = ['test_http_signaler.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/python/twitter/mesos/common:http_signaler'),
  ]
)
