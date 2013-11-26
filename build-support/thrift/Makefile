# Copyright 2013 Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
THRIFT_VERSION = 0.5.0
THRIFT_DISTNAME = thrift-$(THRIFT_VERSION)
THRIFT_URL = http://archive.apache.org/dist/incubator/thrift/$(THRIFT_VERSION)-incubating/$(THRIFT_DISTNAME).tar.gz

THRIFT = ./thrift-$(THRIFT_VERSION)/compiler/cpp/thrift

all: $(THRIFT)

$(THRIFT):
	curl $(THRIFT_URL) | tar zxv
	(cd $(THRIFT_DISTNAME) && ./configure --without-php --without-php_extension --without-ruby --without-haskell)
	(cd $(THRIFT_DISTNAME) && make -j4)

clean:
	$(MVN) clean
	rm -fr $(THRIFT_DISTNAME)
