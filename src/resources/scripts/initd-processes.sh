#!/bin/sh

# Shell functions to start/stop Bigdata processes using SysV init

# Function called by /etc/init.d/bigdata to start the Bigdata processes
do_start() {
    log_begin_msg "Starting Bigdata processes"

    BIGDATA_VAR="${BIGDATA_HOME}/var"
    DEPLOY_FILE="${BIGDATA_VAR}/config/deploy/deploy.properties"
    DEFAULTS_FILE="${BIGDATA_VAR}/config/deploy/default-deploy.properties"

    chown "${BIGDATA_USER}":"${BIGDATA_USER}" "${BIGDATA_VAR}"
    sudo -u "${BIGDATA_USER}" mkdir -p "${BIGDATA_VAR}/log"
    sudo -u "${BIGDATA_USER}" mkdir -p "${BIGDATA_VAR}/state"

    # Set TCP/IP parameters
    echo 20 > /proc/sys/net/ipv4/tcp_keepalive_time
    echo 1 > /proc/sys/net/ipv4/tcp_keepalive_intvl
    echo 9 > /proc/sys/net/ipv4/tcp_keepalive_probes
    echo 6 > /proc/sys/net/ipv4/tcp_retries2

    # Set location & name of core files

    BIGDATA_CORES="${BIGDATA_VAR}/log/cores"

    # 1. Write 0 to the core_uses_pid to generate core files named
    #    "core" rather than "core.pid"
    echo 0 > /proc/sys/kernel/core_uses_pid || exit 1

    # 2. Want to dump all core files to var/log/cores, so must make sure that
    #    directory exits
    [ -d "${BIGDATA_CORES}" ] || sudo -u "${BIGDATA_USER}" mkdir "${BIGDATA_CORES}" || exit 1

    # 3. Write the desired pattern to core_pattern so that when a core file is
    #    generated, it will be written to the file /var/cores/core.<exe>.<time>
    #    where <exe> is the filename of the executable that core dumped,
    #    and <time> is the number of seconds since 0:00h, 1 Jan 1970 
    echo "${BIGDATA_CORES}/core.%e.%t" > /proc/sys/kernel/core_pattern || exit 1

    # Execute the boot launcher in the background, and so that it
    # restarts on reboot
    nohup start-stop-daemon --start --quiet \
        --make-pidfile --pidfile "${BIGDATA_PGRP}" \
        --chuid "${BIGDATA_USER}" --chdir "${BIGDATA_HOME}" \
        --exec "${BIGDATA_HOME}/bin/launcher" > /dev/null 2>&1 &

    log_end_msg $?
}

# Function called by /etc/init.d/bigdata to stop the Bigdata processes
do_stop() {
    log_begin_msg "Stopping Bigdata processes"

    if [ -f "${BIGDATA_PGRP}" ]; then

        # First try a clean shutdown
        "${BIGDATA_HOME}/bin/boot-tool" stop

        # Kill everything in process group if not successful
        if [ $? -ne 0 ]; then
            /usr/bin/pkill -9 -g `cat "${BIGDATA_PGRP}"` || true
        fi
        rm -f "${BIGDATA_PGRP}"
    fi

    log_end_msg $?
}

# Function called by /etc/init.d/bigdata to retrieve status of the 
# Bigdata processes
do_status() {
    "${BIGDATA_HOME}/bin/boot-tool" status 2>/dev/null |grep -v "STOPPED"
}
