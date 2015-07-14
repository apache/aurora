Building Aurora RPMs
====================

This directory contains all necessary scripting to support the building of Aurora
nightly and release RPMs.  Building and deployment have been tested against the following
Red Hat flavors:

 * CentOS 6/7 on x86_64
 * Fedora 19/20 on x86_64

How to build using Make and rpmbuild
------------------------------------

1. Install the necessary build dependencies via yum-builddep:

```bash
cd build-support/packaging/rpm
sudo yum install -y make rpm-build yum-utils
make srpm
sudo yum-builddep ../../../dist/rpmbuild/SRPMS/*
```

2. Build the RPM via Make.

```bash
make rpm
```

3. After the RPM building process has concluded, RPMs will land here:

```
$AURORA_HOME/dist/rpmbuild/RPMS
```
