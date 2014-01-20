#
# Copyright 2013 Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

python_test_suite(name = 'all',
  dependencies = [
    pants(':test_base'),
    pants(':test_constraint_parsing'),
    pants(':test_loader'),
    pants(':test_thrift'),
  ]
)

python_tests(name = 'test_base',
  sources = ['test_base.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('src/main/python/apache/aurora/config'),
  ]
)

python_tests(name = 'test_loader',
  sources = ['test_loader.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('src/main/python/apache/aurora/config'),
  ],
  coverage = [
    'apache.aurora.config',
    'apache.thermos.config'
  ]
)

python_tests(name = 'test_constraint_parsing',
  sources = ['test_constraint_parsing.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/config'),
  ]
)

python_tests(name = 'test_thrift',
  sources = ['test_thrift.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/config'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift-test'),
  ]
)
