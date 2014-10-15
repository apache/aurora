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

THRIFT_VERSION = 0.9.1
THRIFT_DISTNAME = thrift-$(THRIFT_VERSION)
THRIFT_URL = http://www.us.apache.org/dist/thrift/$(THRIFT_VERSION)/$(THRIFT_DISTNAME).tar.gz
#TODO(zmanji): Verify the MD5 of the downloaded file.

THRIFT = ./thrift-$(THRIFT_VERSION)/compiler/cpp/thrift

CONFIGURE_FLAGS = --disable-dependency-tracking \
  --disable-shared \
  --without-c_glib \
  --without-cpp \
  --without-csharp \
  --without-d \
  --without-erlang \
  --without-go \
  --without-java \
  --without-haskell \
  --without-perl \
  --without-php \
  --without-php_extension \
  --without-pic \
  --without-python \
  --without-qt4 \
  --without-ruby \
  --without-tests

all: $(THRIFT)

$(THRIFT):
	curl -s $(THRIFT_URL) | tar zxv
	(cd $(THRIFT_DISTNAME) && ./configure $(CONFIGURE_FLAGS) && make -j4)

clean:
	rm -fr $(THRIFT_DISTNAME)
