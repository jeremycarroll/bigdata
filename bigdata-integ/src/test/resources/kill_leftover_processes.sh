#! /bin/bash
#
# Kill all instances of the java QuorumPeerMain process
# TODO: Make this more generic so that we can kill processes
#       with other names as well
#
kill `jps | grep QuorumPeerMain | cut -d' ' -f1 `
