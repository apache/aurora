Java code in the aurora repo is built with [Gradle](http://gradle.org).


Prerequisite
============

When using Apache Aurora checked out from the source repository or the binary
distribution, the Gradle wrapper and JavaScript dependencies are provided.
However, you need to manually install them when using the source release
downloads:

1. Install Gradle following the instructions on the [Gradle web site](http://gradle.org)
2. From the root directory of the Apache Aurora project generate the gradle
wrapper by running:

    gradle wrapper


Getting Started
===============

You will need Java 8 installed and on your `PATH` or unzipped somewhere with `JAVA_HOME` set. Then

    ./gradlew tasks

will bootstrap the build system and show available tasks. This can take a while the first time you
run it but subsequent runs will be much faster due to cached artifacts.

Running the Tests
-----------------
Aurora has a comprehensive unit test suite. To run the tests use

    ./gradlew build

Gradle will only re-run tests when dependencies of them have changed. To force a re-run of all
tests use

    ./gradlew clean build

Running the build with code quality checks
------------------------------------------
To speed up development iteration, the plain gradle commands will not run static analysis tools.
However, you should run these before posting a review diff, and **always** run this before pushing a
commit to origin/master.

    ./gradlew build -Pq

Running integration tests
-------------------------
To run the same tests that are run in the Apache Aurora continuous integration
environment:

    ./build-support/jenkins/build.sh


In addition, there is an end-to-end test that runs a suite of aurora commands
using a virtual cluster:

    ./src/test/sh/org/apache/aurora/e2e/test_end_to_end.sh



Creating a bundle for deployment
--------------------------------
Gradle can create a zip file containing Aurora, all of its dependencies, and a launch script with

    ./gradlew distZip

or a tar file containing the same files with

    ./gradlew distTar

The output file will be written to `dist/distributions/aurora-scheduler.zip` or
`dist/distributions/aurora-scheduler.tar`.

Developing Aurora Java code
===========================

Setting up an IDE
-----------------
Gradle can generate project files for your IDE. To generate an IntelliJ IDEA project run

    ./gradlew idea

and import the generated `aurora.ipr` file.

Adding or Upgrading a Dependency
--------------------------------
New dependencies can be added from Maven central by adding a `compile` dependency to `build.gradle`.
For example, to add a dependency on `com.example`'s `example-lib` 1.0 add this block:

    compile 'com.example:example-lib:1.0'

NOTE: Anyone thinking about adding a new dependency should first familiarize themself with the
Apache Foundation's third-party licensing
[policy](http://www.apache.org/legal/resolved.html#category-x).

Developing Aurora UI
======================

Installing bower (optional)
----------------------------
Third party JS libraries used in Aurora (located at 3rdparty/javascript/bower_components) are
managed by bower, a JS dependency manager. Bower is only required if you plan to add, remove or
update JS libraries. Bower can be installed using the following command:

    npm install -g bower

Bower depends on node.js and npm. The easiest way to install node on a mac is via brew:

    brew install node

For more node.js installation options refer to https://github.com/joyent/node/wiki/Installation.

More info on installing and using bower can be found at: http://bower.io/. Once installed, you can
use the following commands to view and modify the bower repo at
3rdparty/javascript/bower_components

    bower list
    bower install <library name>
    bower remove <library name>
    bower update <library name>
    bower help

Faster Iteration in Vagrant
---------------------------
The scheduler serves UI assets from the classpath. For production deployments this means the assets
are served from within a jar. However, for faster development iteration, the vagrant image is
configured to add `/vagrant/dist/resources/main` to the head of CLASSPATH. This path is configured
as a shared filesystem to the path on the host system where your Aurora repository lives. This means
that any updates to dist/resources/main in your checkout will be reflected immediately in the UI
served from within the vagrant image.

The one caveat to this is that this path is under `dist` not `src`. This is because the assets must
be processed by gradle before they can be served. So, unfortunately, you cannot just save your local
changes and see them reflected in the UI, you must first run `./gradlew processResources`. This is
less than ideal, but better than having to restart the scheduler after every change. Additionally,
gradle makes this process somewhat easier with the use of the `--continuous` flag. If you run:
`./gradlew processResources --continuous` gradle will monitor the filesystem for changes and run the
task automatically as necessary. This doesn't quite provide hot-reload capabilities, but it does
allow for <5s from save to changes being visibile in the UI with no further action required on the
part of the developer.

Developing the Aurora Build System
==================================

Bootstrapping Gradle
--------------------
The following files were autogenerated by `gradle wrapper` using gradle 1.8's
[Wrapper](http://www.gradle.org/docs/1.8/dsl/org.gradle.api.tasks.wrapper.Wrapper.html) plugin and
should not be modified directly:

    ./gradlew
    ./gradlew.bat
    ./gradle/wrapper/gradle-wrapper.jar
    ./gradle/wrapper/gradle-wrapper.properties

To upgrade Gradle unpack the new version somewhere, run `/path/to/new/gradle wrapper` in the
repository root and commit the changed files.

Making thrift schema changes
============================
See [this document](thrift-deprecation.md) for any thrift related changes.
