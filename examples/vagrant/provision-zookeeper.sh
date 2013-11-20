#!/bin/bash
apt-get update
apt-get -y install zookeeper
echo "JVMFLAGS=\"-Djava.net.preferIPv4Stack=true\"" >> /etc/zookeeper/conf/environment
cat > /etc/rc.local <<EOF
#!/bin/sh -e
/usr/share/zookeeper/bin/zkServer.sh start
EOF
chmod +x /etc/rc.local
/etc/rc.local
