
python_binary(
  name='client',
  entry_point = 'apache.aurora.cli:main',
  dependencies = [ pants(':cli') ],
  )

python_library(
  name='cli',
  sources = [ '__init__.py', 'context.py', 'jobs.py', 'options.py' ],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/python'),
    pants('src/main/python/apache/aurora/client/api:command_runner'),
    pants('src/main/python/apache/aurora/client/api:disambiguator'),
    pants('src/main/python/apache/aurora/client/api:job_monitor'),
    pants('src/main/python/apache/aurora/client/api:updater'),
    pants('src/main/python/apache/aurora/client/hooks'),
    pants('src/main/python/apache/aurora/client:base'),
    pants('src/main/python/apache/aurora/client:config'),
    pants('src/main/python/apache/aurora/client:factory'),
    pants('src/main/python/apache/aurora/client:options'),
    pants('src/main/python/apache/aurora/common'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
    pants('src/main/python/apache/aurora:argparse')
    ]
  )
