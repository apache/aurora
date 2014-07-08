Apache Aurora is a service scheduler that runs on top of Apache Mesos, enabling
you to run long-running services that take advantage of Apache Mesos'
scalability, fault-tolerance, and resource isolation. Apache Aurora is
currently a part of the Apache Incubator.

## Getting Started
* [Developing Aurora](docs/developing-aurora-scheduler.md)
* [Deploying Aurora](docs/deploying-aurora-scheduler.md)
* [Running a Local Cluster with Vagrant](docs/vagrant.md)
* More docs coming soon!

## Requiremensts
* Python 2.6 or higher
* JDK 1.7 or higher

* Source distribution requirements
       * [Gradle](http://gradle.org)

## How to build
Gradle and Bower are not shipped with the source distribution of Apache Aurora.
The following instructions apply for the source release downloads only. When
using Apache Aurora checked out from the source repository or the binary
distribution the gradle wrapper and javascript dependencies are provided.

1. Install Gradle following the instructions on the [Gradle web site](http://gradle.org)
2. From the root directory of the Apache Aurora project generate the gradle
wrapper by running

		gradle wrapper

### Testing
To run the same tests that are run in the Apache Aurora continuous integration
environment

* From the root directory of the Apache Aurora project run:

		./build-support/jenkins/build.sh

### Compiling the source packages
To compile the source packages into binary distributions

* From the root directory of the Apache Aurora project run:

		./gradlew distTar
		./build-support/release/make-python-sdists

For additional information see the [Developing Aurora](docs/developing-aurora-scheduler.md)
guide.

## License
Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
