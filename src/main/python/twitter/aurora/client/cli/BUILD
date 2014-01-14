
python_binary(
  name='client',
  entry_point = 'twitter.aurora.client.cli:main',
  dependencies = [ pants(':cli') ],
  )

python_library(
  name='cli',
  sources = [ '__init__.py', 'context.py', 'jobs.py' ],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/python'),
    pants('src/main/python/twitter/aurora/client/api:command_runner'),
    pants('src/main/python/twitter/aurora/client/api:disambiguator'),
    pants('src/main/python/twitter/aurora/client/api:job_monitor'),
    pants('src/main/python/twitter/aurora/client/api:updater'),
    pants('src/main/python/twitter/aurora/client/hooks'),
    pants('src/main/python/twitter/aurora/client:base'),
    pants('src/main/python/twitter/aurora/client:config'),
    pants('src/main/python/twitter/aurora/client:factory'),
    pants('src/main/python/twitter/aurora/client:options'),
    pants('src/main/python/twitter/aurora/common'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('src/main/python/twitter/aurora:argparse')
    ]
  )
