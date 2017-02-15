#!/bin/bash
echo "Checking volume mounts..."
if [ ! -f "/etc/rsyslog.d.container/50-default.conf" ]; then
    echo "Mounted file was not found";
    exit 1
fi
