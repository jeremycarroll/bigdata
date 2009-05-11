#!/bin/bash

source `dirname $0`/bigdataenv

# Run a master that generates an LUBM data set using clients that are
# distributed across the federation.  The parameters for the data set
# to be generated are read from the specified configuration file.
#

java ${JAVA_OPTS} \
    edu.lehigh.swat.bench.ubt.bigdata.LubmGeneratorMaster \
    ${BIGDATA_CONFIG} ${BIGDATA_CONFIG_OVERRIDES}

