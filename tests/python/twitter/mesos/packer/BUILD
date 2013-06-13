python_test_suite(name = 'all',
  dependencies = [
    pants(':packer_client'),
  ],
)

python_tests(name = 'packer_client',
  sources = ['test_packer_client.py'],
  soft_dependencies = True,
  dependencies = [
    pants('src/python/twitter/mesos/packer:packer'),
    pants('3rdparty/python:mox')
  ],
)
