#!/bin/bash

# Runs the LUBM queries as configured using the LUBM test harness.
#
# Pre-conditions: jini is running; services are running.
#
# usage: namespace (of the KB)

source `dirname $0`/bigdataenv

java ${JAVA_OPTS} \
	-cp ${CLASSPATH}:${libDir}/bigdata-lubm.jar \
    -Dlubm.warmUp=false \
    -Dlubm.queryTime=5 \
    -Dlubm.queryParallel=1 \
    -DminDataServices=13 \
    -Dnamespace=$1 \
    edu.lehigh.swat.bench.ubt.Test \
    query \
    /opt2/src/config.kb.bigdataCluster \
    /opt2/src/bigdata-lubm/src/java/edu/lehigh/swat/bench/ubt/bigdata/config.query9.sparql

#config.query-1-14-9.sparql

exit

#   config.query.sparql
#   config.query-1-14-9.sparql
#   config.query9.sparql
