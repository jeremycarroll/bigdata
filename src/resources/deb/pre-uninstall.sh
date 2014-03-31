#!/bin/sh
set -e

[ -f /etc/default/bigdata ] && . /etc/default/bigdata

stopBigdata() {
        service bigdata stop || true
}

case "$1" in
    upgrade)
        if [ "$RESTART_ON_UPGRADE" = "true" ] ; then
                stopBigdata
        fi
    ;;

    remove)
        stopBigdata
    ;;

    deconfigure|failed-upgrade)
    ;;

    *)
        echo "$0 called with unknown argument \`$1'" >&2
        exit 1
    ;;
esac
