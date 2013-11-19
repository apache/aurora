python_binary(
  name = 'aurora_client',
  source = 'aurora_client.py',
  entry_point = 'twitter.aurora.client.bin.aurora_client:proxy_main',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/twitter/aurora/client/commands:all'),
    pants('src/main/python/twitter/aurora/client:base'),
  ]
)

python_binary(
  name = 'aurora_admin',
  source = 'aurora_admin.py',
  entry_point = 'twitter.aurora.client.bin.aurora_admin:proxy_main',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/twitter/aurora/client/commands:admin'),
    pants('src/main/python/twitter/aurora/client/commands:help'),
    pants('src/main/python/twitter/aurora/client:base'),
    pants('src/main/python/twitter/aurora/client:options'),
  ]
)
