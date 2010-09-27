#! /bin/bash
kill `jps | grep QuorumPeerMain | cut -d' ' -f1 `
