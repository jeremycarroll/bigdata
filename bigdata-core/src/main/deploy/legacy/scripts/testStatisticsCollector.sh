#!/bin/bash

# Tests the statistics collector for the platform.
#
# usage: [interval [count]]
#
# See com.bigdata.counters.httpd.AbstractStatisticsCollector#main(String[])

source `dirname $0`/bigdataenv

java ${JAVA_OPTS} \
	-cp ${CLASSPATH} \
    com.bigdata.counters.httpd.AbstractStatisticsCollector \
    $*
