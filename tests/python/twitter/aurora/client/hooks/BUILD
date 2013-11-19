python_test_suite(name = 'all',
  dependencies = [
    pants(':hooked_api'),
    pants(':non_hooked_api'),
  ],
)

python_tests(name = 'hooked_api',
  sources = ['test_hooked_api.py'],
  dependencies = [
    pants('src/python/twitter/aurora/common:cluster'),
    pants('src/python/twitter/aurora/client:api'),
    pants('src/python/twitter/aurora/client/hooks'),
    pants('src/python/twitter/aurora/BUILD.thirdparty:mock'),
  ]
)

python_tests(name = 'non_hooked_api',
  sources = ['test_non_hooked_api.py'],
  dependencies = [
    pants('src/python/twitter/aurora/common:aurora_job_key'),
    pants('src/python/twitter/aurora/client/hooks'),
  ],
)
