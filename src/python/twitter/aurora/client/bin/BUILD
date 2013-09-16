python_library(
  name = 'aurora_client_source',
  sources = ['aurora_client.py'],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/aurora/client:base'),
    pants('src/python/twitter/aurora/client/commands:all'),
  ]
)

python_binary(
  name = 'aurora_client',
  entry_point = 'twitter.aurora.client.bin.aurora_client',
  dependencies = [
    pants(':aurora_client_source'),
  ]
)
