#!/bin/sh
VERSION=`basename ant-build/lib/bigdata-*syapse*.jar .jar | sed -e 's/^bigdata-//'`
fpm -s dir -t deb -n bigdata -v ${VERSION} \
    --description "Systap Bigdata triple store database" \
    --url "https://github.com/syapse/bigdata" \
    --depends oracle-java7-jre \
    --depends java7-runtime-headless \
    --architecture all \
    --deb-upstart src/resources/deb/bigdata \
    --after-install src/resources/deb/post-install.sh \
    --before-remove src/resources/deb/pre-uninstall.sh \
    --after-remove src/resources/deb/post-uninstall.sh \
    ant-build/lib=/usr/share/bigdata/ \
    src/resources/deb/etc=/ \
    src/resources/deb/usr=/
