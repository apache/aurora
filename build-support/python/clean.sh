#!/bin/bash

set -ux

AURORA_BASE=$(dirname "$0")/../..
rm -rf "$HOME/.pex"
rm -rf "$AURORA_BASE/.pants.d"
rm -rf "$AURORA_BASE/.python"
rm -f  "$AURORA_BASE/pants.pex"
find "$AURORA_BASE" -name '*.pyc' | xargs rm -f
