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


GIT_COMMIT ?= HEAD

USING_GIT ?= $(shell git status >/dev/null 2>&1 && echo 'true' || echo 'false')
ifeq ($(USING_GIT),true)
	VERSION_SUFFIX := .$(shell git rev-parse --short $(GIT_COMMIT))
else
	VERSION_SUFFIX :=
endif

CURRENT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

TOPDIR ?= $(CURRENT_DIR)/../../..
DIST_DIR ?= $(TOPDIR)/dist
RPM_TOPDIR ?= $(DIST_DIR)

DATETIME ?= $(shell date +%Y%m%d%H%M)

SOURCE_TARGET ?= $(RPM_TOPDIR)/rpmbuild/SOURCES/aurora.tar.gz

AURORA_VERSION ?= $(shell cat $(TOPDIR)/.auroraversion | tr '-' '.')
MESOS_VERSION ?= $(shell sed -n 's/.*mesos\.native==\(.*\)/\1/p' $(TOPDIR)/3rdparty/python/requirements.txt)


PHONY: all nightly_version release_version get_source_target clean mkdir srpm nightly_srpm nightly_rpm release_srpm release_rpm tar_source

all: release_rpm

nightly_version:
	$(eval AURORA_VERSION := $(AURORA_VERSION).$(DATETIME)$(VERSION_SUFFIX))

release_version:
	$(eval AURORA_VERSION := $(AURORA_VERSION))

clean:
	rm -rf $(RPM_TOPDIR)/rpmbuild

mkdir: clean
	mkdir -p $(RPM_TOPDIR)/rpmbuild/BUILD
	mkdir -p $(RPM_TOPDIR)/rpmbuild/BUILDROOT
	mkdir -p $(RPM_TOPDIR)/rpmbuild/RPMS
	mkdir -p $(RPM_TOPDIR)/rpmbuild/SOURCES
	mkdir -p $(RPM_TOPDIR)/rpmbuild/SRPMS

tar_source: mkdir
	if [[ "$(USING_GIT)" == 'true' ]]; then \
		cd `git rev-parse --show-toplevel` && git archive --format=tar --prefix=aurora/ $(GIT_COMMIT) | gzip > $(SOURCE_TARGET); \
	else \
		tmp_dir=`mktemp -d '/tmp/aurorabuild.XXXXXX'`; \
		mkdir $${tmp_dir}/aurora; \
		cp -r $(TOPDIR)/* $${tmp_dir}/aurora; \
		cd $${tmp_dir}; \
		tar cvzf $(SOURCE_TARGET) aurora; \
		rm -rf $${tmp_dir}; \
	fi

srpm: tar_source
	rpmbuild $(RPM_OPTS) \
		--define "_topdir $(RPM_TOPDIR)" \
		--define "_builddir %{_topdir}/rpmbuild/BUILD" \
		--define "_buildrootdir %{_topdir}/rpmbuild/BUILDROOT" \
		--define "_rpmdir %{_topdir}/rpmbuild/RPMS" \
		--define "_srcrpmdir %{_topdir}/rpmbuild/SRPMS" \
		--define "_specdir %(pwd)" \
		--define "_sourcedir  %{_topdir}/rpmbuild/SOURCES" \
		--define "AURORA_VERSION $(AURORA_VERSION)" \
		--define "MESOS_VERSION $(MESOS_VERSION)" \
		--nodeps -bs aurora.spec

rpm: tar_source
	rpmbuild $(RPM_OPTS) \
		--define "_topdir $(RPM_TOPDIR)" \
		--define "_builddir %{_topdir}/rpmbuild/BUILD" \
		--define "_buildrootdir %{_topdir}/rpmbuild/BUILDROOT" \
		--define "_rpmdir %{_topdir}/rpmbuild/RPMS" \
		--define "_srcrpmdir %{_topdir}/rpmbuild/SRPMS" \
		--define "_specdir %(pwd)" \
		--define "_sourcedir  %{_topdir}/rpmbuild/SOURCES" \
		--define "AURORA_VERSION $(AURORA_VERSION)" \
		--define "MESOS_VERSION $(MESOS_VERSION)" \
		-ba aurora.spec

nightly_srpm: nightly_version srpm

nightly_rpm: nightly_version rpm

release_srpm: release_version srpm

release_rpm: release_version rpm
