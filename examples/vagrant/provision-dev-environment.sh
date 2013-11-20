export TAG=0.15.0-rc4

apt-get update
apt-get -y install \
    git automake libtool g++ java7-runtime-headless curl \
    openjdk-7-jdk python-dev libsasl2-dev libcurl4-openssl-dev \
    make

echo Cloning aurora repo
if [ ! -d aurora ]; then
  git clone /vagrant aurora
fi

pushd aurora
  mkdir -p third_party
  git pull
popd

echo Cloning mesos repo

if [ ! -d mesos ]; then
  git clone https://git-wip-us.apache.org/repos/asf/mesos.git
fi

if [ ! -f mesos-build/src/python/dist/*.egg ]; then
  pushd mesos
    git checkout $TAG
    sed -i~ "s/\[mesos\], \[.*\]/[mesos], [$TAG]/" configure.ac
    [[ -f ./configure ]] || ./bootstrap
  popd

  mkdir -p mesos-build
  pushd mesos-build
    ../mesos/configure
    make -j3
    cp src/python/dist/*.egg ../aurora/third_party
  popd
fi

pushd aurora
  # build scheduler
  ./gradlew distTar

  # build clients
  ./pants src/main/python/twitter/aurora/client/bin:aurora_admin
  ./pants src/main/python/twitter/aurora/client/bin:aurora_client

  # build executors/observers
  ./pants src/main/python/twitter/aurora/executor/bin:gc_executor
  ./pants src/main/python/twitter/aurora/executor/bin:thermos_executor
  ./pants src/main/python/twitter/aurora/executor/bin:thermos_runner
  ./pants src/main/python/twitter/thermos/observer/bin:thermos_observer

  # package runner w/in executor
  python <<EOF
import contextlib
import zipfile
with contextlib.closing(zipfile.ZipFile('dist/thermos_executor.pex', 'a')) as zf:
  zf.writestr('twitter/aurora/executor/resources/__init__.py', '')
  zf.write('dist/thermos_runner.pex', 'twitter/aurora/executor/resources/thermos_runner.pex')
EOF

  mkdir -p /vagrant/dist/distributions
  cp dist/distributions/aurora-scheduler.tar /vagrant/dist/distributions

  for pex in aurora_admin aurora_client gc_executor thermos_executor thermos_observer; do
    cp dist/$pex.pex /vagrant/dist
  done
popd

sudo chown -R vagrant.vagrant mesos mesos-build aurora .pex
