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
THRIFT_URL = https://archive.apache.org/dist/thrift/$(THRIFT_VERSION)/$(THRIFT_DISTNAME).tar.gz
THRIFT_SHA256 = ac175080c8cac567b0331e394f23ac306472c071628396db2850cb00c41b0017

# This patch is documented here: https://issues.apache.org/jira/browse/THRIFT-2386
THRIFT_PATCH_URL = https://issues.apache.org/jira/secure/attachment/12632477/yylex.patch
THRIFT_PATCH_SHA256 = 70f20b4e5b2e004b8a0d075b80a52750bce5be02ed83efdc60adbc45ec386a6c

THRIFT = ./$(THRIFT_DISTNAME)/compiler/cpp/thrift
THRIFT_CONFIGURE_FLAGS = \
  --disable-dependency-tracking \
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
  --without-qt4 \
  --without-ruby \
  --without-tests

all: $(THRIFT)

define calculate_sha256
	openssl dgst -sha256 | cut -d' ' -f2
endef

$(THRIFT):
	sha256=$$(curl -s --show-error $(THRIFT_URL) | tee $(THRIFT_DISTNAME).tar.gz | $(calculate_sha256)) && \
	[ "$${sha256}" = "$(THRIFT_SHA256)" ] && \
	tar zxvf $(THRIFT_DISTNAME).tar.gz && \
	cd $(THRIFT_DISTNAME) && \
	sha256=$$(curl -s --show-error $(THRIFT_PATCH_URL) | tee thrift.patch | $(calculate_sha256)) && \
	[ "$${sha256}" = "$(THRIFT_PATCH_SHA256)" ] && \
	patch -p1 < thrift.patch && \
	patch -p1 < ../AURORA-1727.compiler.cpp.Makefile.in.patch && \
	patch -p1 < ../AURORA-1727.compiler.cpp.src.generate.t_java_generator.cc.patch && \
	patch -p1 < ../AURORA-1727.lib.py.setup.py.patch && \
	./configure $(THRIFT_CONFIGURE_FLAGS) && \
	make -j4

clean:
	rm -fr $(THRIFT_DISTNAME)*
