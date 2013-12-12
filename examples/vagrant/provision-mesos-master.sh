#!/bin/bash -x
apt-get update
apt-get -y install java7-runtime-headless curl
wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_0.15.0-rc4_amd64.deb
dpkg --install mesos_0.15.0-rc4_amd64.deb
cat > /usr/local/sbin/mesos-master.sh <<EOF
#!/bin/bash
export LD_LIBRARY_PATH=/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server
(
  while true
  do
    /usr/local/sbin/mesos-master --zk=zk://192.168.33.2:2181/mesos/master --ip=192.168.33.3
    echo "Master exited with $?, restarting."
  done
) & disown
EOF
chmod +x /usr/local/sbin/mesos-master.sh

cat > /etc/rc.local <<EOF
#!/bin/sh -e
/usr/local/sbin/mesos-master.sh >/var/log/mesos-master-stdout.log 2>/var/log/mesos-master-stderr.log
EOF
chmod +x /etc/rc.local

/etc/rc.local
