#!/bin/bash -x
apt-get update
apt-get -y install java7-runtime-headless curl
wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_0.15.0_amd64.deb
dpkg --install mesos_0.15.0_amd64.deb

# Get the IP address; eth1 is the net interface we care about.
export SLAVE_IP_ADDRESS=$(ip addr show eth1 | grep 'inet ' | cut -d'/' -f 1 | cut -c '10-')
cat> /usr/local/sbin/mesos-slave.sh <<EOF
#!/bin/bash
export LD_LIBRARY_PATH=/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server
(
  while true
  do
    # TODO(ksweeney): Scheduler assumes 'rack' and 'host' are present. Make them optional.
    /usr/local/sbin/mesos-slave --master=zk://192.168.33.2:2181/mesos/master --ip=$SLAVE_IP_ADDRESS \
      --attributes='host:$SLAVE_IP_ADDRESS;rack:a' --resources="cpus:4;mem:1024;disk:20000"
    echo "Master exited with \$?, restarting."
  done
) & disown
EOF
chmod +x /usr/local/sbin/mesos-slave.sh

cat > /usr/local/bin/thermos_observer.sh <<"EOF"
#!/bin/bash
(
  while true
  do
    /usr/local/bin/thermos_observer \
         --root=/var/run/thermos \
         --port=1338 \
         --log_to_disk=NONE \
         --log_to_stderr=google:INFO
    echo "Observer exited with $?, restarting."
    sleep 10
  done
) & disown
EOF
chmod +x /usr/local/bin/thermos_observer.sh

# TODO(ksweeney): Hack until the --hostname change for mesos-slave lands.
echo $SLAVE_IP_ADDRESS > /etc/hostname
hostname $SLAVE_IP_ADDRESS

# TODO(ksweeney): Replace with public and versioned URLs.
install -m 755 /vagrant/dist/gc_executor.pex /usr/local/bin/gc_executor
install -m 755 /vagrant/dist/thermos_executor.pex /usr/local/bin/thermos_executor
install -m 755 /vagrant/dist/thermos_observer.pex /usr/local/bin/thermos_observer

cat > /etc/rc.local <<EOF
#!/bin/sh -e
/usr/local/sbin/mesos-slave.sh >/var/log/mesos-slave-stdout.log 2>/var/log/mesos-slave-stderr.log
/usr/local/bin/thermos_observer.sh >/var/log/thermos-observer.log 2>&1
EOF
chmod +x /etc/rc.local

/etc/rc.local
