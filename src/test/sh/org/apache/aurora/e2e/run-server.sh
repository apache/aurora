#!/bin/bash
echo "Starting up server..."
while true
do
  echo -e "HTTP/1.1 200 OK\r\n\r\nHello from a filesystem image." | nc -l "$1"
done
