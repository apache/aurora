Developing the Aurora Scheduler UI
==================================

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
configured to add the `scheduler` subtree of `/vagrant/dist/resources/main` to the head of
`CLASSPATH`. This path is configured as a shared filesystem to the path on the host system where
your Aurora repository lives. This means that any updates under `dist/resources/main/scheduler` in
your checkout will be reflected immediately in the UI served from within the vagrant image.

The one caveat to this is that this path is under `dist` not `src`. This is because the assets must
be processed by gradle before they can be served. So, unfortunately, you cannot just save your local
changes and see them reflected in the UI, you must first run `./gradlew processResources`. This is
less than ideal, but better than having to restart the scheduler after every change. Additionally,
gradle makes this process somewhat easier with the use of the `--continuous` flag. If you run:
`./gradlew processResources --continuous` gradle will monitor the filesystem for changes and run the
task automatically as necessary. This doesn't quite provide hot-reload capabilities, but it does
allow for <5s from save to changes being visibile in the UI with no further action required on the
part of the developer.
