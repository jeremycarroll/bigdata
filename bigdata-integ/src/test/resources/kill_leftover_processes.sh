#! /bin/bash
#
# Kill all instances of the java QuorumPeerMain and ServiceStarter processes.
# This is a bit dangerous in a CI environment as we might have other build in progress
# that have launched these Java processes.  This should suffice for the short-term though.
#
# TODO: Make this more generic so that we can kill processes
#       with other names as well.
#
kill `jps | grep QuorumPeerMain | cut -d' ' -f1 `
kill `jps | grep ServiceStarter | cut -d' ' -f1 `
