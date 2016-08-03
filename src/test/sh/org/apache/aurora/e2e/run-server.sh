#!/bin/sh

echo "Starting up server..."
while true
do
  echo -e "HTTP/1.1 200 OK\n\n Hello from a filesystem image" | nc -l "$1"
done
