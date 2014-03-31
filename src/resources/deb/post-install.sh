#!/bin/sh
set -e

[ -f /etc/default/bigdata ] && . /etc/default/bigdata

startBigdata() {
        service bigdata start || true
}

case "$1" in
    configure)
        [ -z "$BD_USER" ] && BD_USER="bigdata"
        [ -z "$BD_GROUP" ] && BD_GROUP="bigdata"
        if ! getent group "$BD_GROUP" > /dev/null 2>&1 ; then
            addgroup --system "$BD_GROUP" --quiet
        fi
        if ! id $BD_USER > /dev/null 2>&1 ; then
            adduser --system --home /usr/share/bigdata --no-create-home \
                --ingroup "$BD_GROUP" --disabled-password --shell /bin/false \
                "$BD_USER"
        fi

        # Set user permissions on /var/log/bigdata, /var/lib/bigdata and /var/run/bigdata
        mkdir -p /var/log/bigdata /var/lib/bigdata /var/run/bigdata
        chown -R $BD_USER:$BD_GROUP /var/log/bigdata /var/lib/bigdata /var/run/bigdata
        chmod 755 /var/log/bigdata /var/lib/bigdata /var/run/bigdata

        # configuration files should not be modifiable by bigdata user, as this can be a security issue
        chown -Rh root:root /etc/bigdata/*
        chmod 755 /etc/bigdata
        find /etc/bigdata -type f -exec chmod 644 {} ';'
        find /etc/bigdata -type d -exec chmod 755 {} ';'

        service rsyslog restart

        # if $2 is set, this is an upgrade
        if ( [ -n $2 ] && [ "$RESTART_ON_UPGRADE" = "true" ] ) ; then
            startBigdata
            # this is a fresh installation
        elif [ -z $2 ] ; then
            echo "### NOT starting bigdata after install, please execute"
            echo " sudo service bigdata start"
        fi
    ;;

    abort-upgrade|abort-remove|abort-deconfigure)
    ;;

    *)
        echo "$0 called with unknown argument \`$1'" >&2
        exit 1
    ;;
esac
