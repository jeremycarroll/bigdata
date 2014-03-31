#!/bin/sh
set -e

case "$1" in
    remove)
        # Remove logs and pids
        rm -rf /var/log/bigdata /var/run/bigdata

        # remove **only** empty data dir
        rmdir -p --ignore-fail-on-non-empty /var/lib/bigdata
    ;;

    purge)
        # Remove service
        update-rc.d bigdata remove >/dev/null || true

        # Remove logs and data
        rm -rf /var/log/bigdata /var/lib/bigdata /var/run/bigdata

        # Remove user/group
        deluser bigdata || true
        delgroup bigdata || true
    ;;

    upgrade|failed-upgrade|abort-install|abort-upgrade|disappear)
        # Nothing to do here
    ;;

    *)
        echo "$0 called with unknown argument \`$1'" >&2
        exit 1
    ;;
esac
