/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
package com.bigdata.process;

import com.bigdata.boot.BootManager;
import com.bigdata.boot.ProcessState;
import com.bigdata.util.Format;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

class ProcessStateMachine implements ProcessManagement {

    public static enum State {
        /** Not running - intentionally stopped. */
        STOPPED,

        /** Starting a process - waiting for a previous stop to complete. */
        STARTING_WAITSTOPPED,

        /** Waiting for all start dependencies to start */
        STARTING_WAITDEPS,

        /** Waiting for the boot state to transition to STARTING. */
        STARTING_WAITSTARTING,

        /** Waiting for the boot state to transition to RUNNING. */
        STARTING_WAITRUNNING,

        /** Process is running. */
        RUNNING,

        /** Process is being stopped. */
        STOPPING,

        /** Process is stopping to be restarted. */
        RESTART_STOPPING,

        /** Process has stopped so it can be restarted. */
        RESTART_STOPPED,

        /** Process is not running because it crashed. */
        FAILED,

        /** An internal error occurred - stop all processing. */
        INTERNAL_ERROR;
    }

    private static final Logger logger =
                                Logger.getLogger(ProcessStateMachine.class);


    // The boot launcher process tag of this process.
    private final String tag;

    // Interface to the process manager service and the rest of the system.
    private Environment env;

    // Global process synchronization object.
    private Object gpSync;

    // Flag indicating if the initialize() method has completed yet.
    private boolean initialized;

    // Flag indicating if the process should be running or not.
    private boolean shouldBeRunning;

    // The state of the process.
    private State state;

    // Flag that's toggled to true every time the state changes.
    private boolean stateChangedFlag;

    // Queue for holding state update messages.
    private final Queue<EventMessage> evQueue =
                      new ConcurrentLinkedQueue<EventMessage>();

    // Set of processes which have registered as dependents. These processes
    // will be notified when the state of this parent process changes.
    private final List<ProcessManagement> dependents =
                      new CopyOnWriteArrayList<ProcessManagement>();

    // The restart group to which this process belongs.
    private RestartGroup restartGroup;

    // Number of times this process has restarted in the last restart interval.
    private int restartCountBin;

    // configuration
    private int autorestartLimitCount;
    private int autorestartLimitInterval;

    // The set of processes that must be running before this process can
    // be started.
    private final Map<String, ProcessManagement> startDependencies =
                      new TreeMap<String, ProcessManagement>();

    // Time at which this process was last started.
    private Date lastStartTime;

    // Total number of times this process has failed.
    private long failureCount;

    ProcessStateMachine(String tag) {
        this.tag = tag;
    }

    // Required by ProcessManagement

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public boolean isRunning() {
        if( !Thread.holdsLock(gpSync) ) {
            throw new IllegalStateException("thread does not hold gpSync");
        }
        return (state == State.RUNNING);
    }

    @Override
    public void setAutoStart(boolean value) {
        if(initialized) {
            throw new IllegalStateException("already initialized");
        }
        this.shouldBeRunning = value;
    }

    @Override
    public void setAutoRestartRate(int count, int interval) {
        if(initialized) {
            throw new IllegalStateException("already initialized");
        }
        this.autorestartLimitCount = count;
        this.autorestartLimitInterval = interval;
    }

    @Override
    public void addStartDependency(String pTag) {
        if(initialized) {
            throw new IllegalStateException("already initialized");
        }
        startDependencies.put(pTag, null);
    }

    @Override
    public void setRestartGroup(RestartGroup rGroup) {
        if(initialized) {
            throw new IllegalStateException("already initialized");
        }
        if(restartGroup != null) {
            throw new IllegalStateException
                          ("process already assigned to a restart group "
                           +"[process='"+tag+"']");
        }
        this.restartGroup = rGroup;
    }

    @Override
    public void initialize(Environment env, Object gpSync)
                    throws InitializationException, IOException
    {
        if(env == null) throw new NullPointerException("null env");
        if(gpSync == null) throw new NullPointerException("null gpSync");

        if( !Thread.holdsLock(gpSync) ) {
            throw new IllegalStateException("thread does not hold gpSync");
        }
        if(initialized) throw new IllegalStateException("already initialized");

        this.env = env;
        this.gpSync = gpSync;

        ProcessState bootState = env.getProcessState(tag);

        switch(bootState) {
            case STOPPED:
                this.state = State.STOPPED;
                break;
            case RUNNING:
                this.state = State.RUNNING;
                break;
            case STARTING:
                this.state = State.STARTING_WAITRUNNING;
                break;
            case STOPPING:
                this.state = State.STOPPING;
                break;
            case FAILED:
                this.state = State.FAILED;
                break;
            default:
                throw new IllegalStateException
                              ("unhandled boot state ["+bootState+"]");
        }

        if(restartGroup == null) {
            // This process will has own group of one
            restartGroup = new RestartGroup();
            restartGroup.addMember(this);
        }

        // Look up and register as a dependent for all processes for which
        // there is a start dependency.
        for( Map.Entry<String, ProcessManagement> e : 
                 startDependencies.entrySet() )
        {
            ProcessManagement pInfo = env.getProcessInfo(e.getKey());
            if(pInfo == null) {
                throw new InitializationException
                              ("unknown process tag listed as a start "
                               +"dependency ['"+e.getKey()+"']");
            }
            e.setValue(pInfo);
            pInfo.registerDependent(this);
        }

        initialized = true;

        // Schedule execution of this state machine for the managed process
        this.wake();
    }

    @Override
    public void registerDependent(ProcessManagement pInfo) {
        dependents.add(pInfo);
    }

    @Override
    public void wake() {
        env.schedule(this);
    }

    @Override
    public void restart_stop() {
        if( !Thread.holdsLock(gpSync) ) {
            throw new IllegalStateException("thread does not hold gpSync");
        }

        switch(state) {
            case STOPPED:
            case FAILED:
            case STARTING_WAITDEPS:
                changeState(State.RESTART_STOPPED);
                break;
            case STARTING_WAITSTARTING:
            case STARTING_WAITRUNNING:
            case RUNNING:
                // Need to stop the process before the restart can proceed
                logger.log(Level.INFO, new Format("Stopping {0}", tag));
                try {
                    env.stopProcess(tag);
                    changeState(State.RESTART_STOPPING);
                    restartGroup.addToStopPendingSet(tag);
                } catch(IOException e) {
                    logger.log(Level.ERROR, 
                               new Format
                                   ("unable to stop process [{0}]",tag), e);
                    changeState(State.INTERNAL_ERROR);
                }
                break;
            case STOPPING:
            case STARTING_WAITSTOPPED:
                // Need to wait for the process to stop before restarting
                changeState(State.RESTART_STOPPING);
                restartGroup.addToStopPendingSet(tag);
                break;
            case INTERNAL_ERROR:
                break;
            default:
                throw new IllegalStateException("unhandled state ["+state+"]");
        }
    }

    @Override
    public void restart_start() {
        if( !Thread.holdsLock(gpSync) ) {
            throw new IllegalStateException("thread does not hold gpSync");
        }

        switch(state) {
            case RESTART_STOPPED:
                changeState(State.STOPPED);
                this.wake();
                break;
            case INTERNAL_ERROR:
                break;
            default:
                throw new IllegalStateException("unhandled state ["+state+"]");
        }
    }

    @Override
    public void onBootStateChange(ProcessState newState) {
        logger.log(Level.TRACE, 
                   new Format("[{0}] onBootStateChange({1})", tag, newState));
        if(initialized) {
            evQueue.add( new BootStateChangeEvent(newState) );
            this.wake();
        }
    }

    @Override
    public void pump() {
        if( !Thread.holdsLock(gpSync) ) {
            throw new IllegalStateException("thread does not hold gpSync");
        }

        boolean notifyDependents = false;

        // Process events
        EventMessage ev;
        while( (ev = evQueue.poll()) != null ) {

            stateChangedFlag = false;
            ev.process();
            if(stateChangedFlag) notifyDependents = true;
        }

        // Check the current state
        do {
            stateChangedFlag = false;
            checkState();
            if(stateChangedFlag) notifyDependents = true;

        } while(stateChangedFlag);

        if(notifyDependents) {
            // Schedule the dependents of this process so those dependents
            // can react to changes in this process
            for(ProcessManagement pInfo : dependents) {
                pInfo.wake();
            }
        }
    }

    public void start() {
        if( !Thread.holdsLock(gpSync) ) {
            throw new IllegalStateException("thread does not hold gpSync");
        }
        this.shouldBeRunning = true;
        this.wake();
    }

    public void stop() {
        if( !Thread.holdsLock(gpSync) ) {
            throw new IllegalStateException("thread does not hold gpSync");
        }
        this.shouldBeRunning = false;
        this.wake();
    }

    private void processBootStateChange(ProcessState bootState) {
        logger.log( Level.DEBUG, 
                    new Format("[{0}] processBootStateChange({1})",
                               tag, bootState) );

        if( !Thread.holdsLock(gpSync) ) {
            throw new IllegalStateException("thread does not hold gpSync");
        }

        // Note: There are a lot of seemingly unnecessary bootState
        // checks below because it's possible for someone to log into
        // the node and manually start and stop processes using boot-tool.

        switch(state) {

            case RUNNING:

                if(bootState == ProcessState.FAILED) {
                    recordProcessFailure();
                } else if(bootState == ProcessState.STOPPING) {
                    // The process was manually stopped
                    this.shouldBeRunning = false;
                    changeState(State.STOPPING);
                }
                break;

            case STARTING_WAITSTOPPED:

                // Failures are ignored when stopping the process
                if( (bootState == ProcessState.STOPPED) ||
                    (bootState == ProcessState.FAILED) )
                {
                    changeState(State.STARTING_WAITDEPS);
                }
                break;

            case STARTING_WAITDEPS:

                if(bootState == ProcessState.STARTING) {
                    // The process was manually started
                    this.shouldBeRunning = true;
                    changeState(State.STARTING_WAITRUNNING);
                }
                break;

            case STARTING_WAITSTARTING:

                if(bootState == ProcessState.STARTING) {
                    changeState(State.STARTING_WAITRUNNING);
                } else if(bootState == ProcessState.RUNNING) {
                    // Shouldn't happen but accept it anyway.
                    changeState(State.RUNNING);
                }
                break;

            case STARTING_WAITRUNNING:

                if(bootState == ProcessState.RUNNING) {
                    changeState(State.RUNNING);
                } else if(bootState == ProcessState.FAILED) {
                    recordProcessFailure();
                } else if(bootState == ProcessState.STOPPING) {
                    // Process was manually stopped
                    this.shouldBeRunning = false;
                    changeState(State.STOPPING);
                }
                break;

            case STOPPING:

                // Failures are ignored when stopping the process
                if( (bootState == ProcessState.STOPPED) ||
                    (bootState == ProcessState.FAILED))
                {
                    changeState(State.STOPPED);
                }
                break;

            case STOPPED:
            case FAILED:

                if(bootState == ProcessState.STARTING) {
                    // The process was manually started
                    this.shouldBeRunning = true;
                    changeState(State.STARTING_WAITRUNNING);
                }
                break;

            case RESTART_STOPPING:

                // Failures are ignored when stopping the process
                if( (bootState == ProcessState.STOPPED) ||
                    (bootState == ProcessState.FAILED))
                {
                    changeState(State.RESTART_STOPPED);
                    restartGroup.removeFromStopPendingSet(tag);
                }
                break;

            case RESTART_STOPPED:

                if(bootState == ProcessState.STARTING) {
                    // Someone is trying to manually start the process;
                    // which can't be allowed.
                    try {
                        env.stopProcess(tag);
                        restartGroup.addToStopPendingSet(tag);
                        changeState(State.RESTART_STOPPING);
                    } catch(IOException e) {
                        logger.log(Level.ERROR, 
                                   new Format("unable to stop already "
                                              +"stopped process [{0}]", tag),
                                   e);
                        changeState(State.INTERNAL_ERROR);
                    }
                }
                break;

            case INTERNAL_ERROR:

                break;

            default:
                throw new IllegalStateException("unhandled state ["+state+"]");
        }
    }

    private void checkState() {

        switch(state) {

            case STARTING_WAITDEPS:

                if( !shouldBeRunning ) {
                    changeState(State.STOPPED);
                    break;
                }

                boolean allDepsRunning = true;
                for(ProcessManagement pInfo : startDependencies.values()) {
                    if( !pInfo.isRunning() ) {
                        allDepsRunning = false;
                        break;
                    }
                }

                if(allDepsRunning) {
                    logger.log( Level.INFO, 
                                new Format("starting process [{0}]", tag) );
                    try {
                        env.startProcess(tag);
                        this.lastStartTime = new Date();
                        changeState(State.STARTING_WAITSTARTING);
                    } catch(IOException e) {
                        logger.warn("unable to start process ['"+tag+"']", e);
                        recordProcessFailure();
                    }
                }
                break;

            case RUNNING:

                if( !shouldBeRunning ) {

                    logger.log( Level.INFO,
                                new Format("stopping process [{0}]", tag) );
                    try {
                        env.stopProcess(tag);
                        changeState(State.STOPPING);
                    } catch(IOException e) {
                        logger.log(Level.ERROR, 
                                   new Format
                                       ("unable to stop process [{0}]", tag),
                                   e);
                        changeState(State.INTERNAL_ERROR);
                    }
                }
                break;

            case STOPPED:
            case FAILED:

                if(shouldBeRunning) changeState(State.STARTING_WAITDEPS);
                break;
        }
    }


    // Records that a process failure has occurred.
    private void recordProcessFailure() {

        if( !Thread.holdsLock(gpSync) ) {
            throw new IllegalStateException("thread does not hold gpSync");
        }

        failureCount++;
        changeState(State.FAILED);

        // The restartCountBin counter is cleared every interval so that
        // the value in the counter is the number of failures in the current
        // interval.

        // Restart is intended to work in the following way:
        //
        //  * One process in a restart group fails
        //  * Each process in the restart group which is starting/running
        //    adds itself to the stopPendingSet and begins to shut down
        //  * As each process stops, the process removes itself from the
        //    stopPendingSet
        //  * Once the stopPendingSet becomes empty, all of the processes
        //    in the restart group are started; and the normal start
        //    dependency logic applies.

        restartCountBin++;
        if(restartCountBin == 1) {
            // Schedule the next interval
            env.delayedSchedule
                (new RestartCountResetTask(), autorestartLimitInterval * 1000);
        }

        if(restartCountBin <= autorestartLimitCount) {

            logger.log(Level.WARN, 
                       new Format("process failed [{0}] - restarting", tag));

            env.sendProcessCrashEvent(tag, true);

            // Restart everything in the restart group
            restartGroup.triggerRestart();

        } else {

            logger.log(Level.WARN, new Format("process failed [{0}] - "
                                              +"restart limit reached", tag) );
            env.sendProcessCrashEvent(tag, false);

            this.shouldBeRunning = false;
        }
    }

    private void changeState(State newState) {

        logger.log(Level.DEBUG, 
                   new Format("[{0}] - process changing state "
                              +"[{1} --> {2}]", tag, state, newState));

        if( !Thread.holdsLock(gpSync) ) {
            throw new IllegalStateException("thread does not hold gpSync");
        }

        if(newState == State.RUNNING) {
            logger.log(Level.INFO, new Format("process is running [{0}]",tag));
        }

        this.state = newState;
        this.stateChangedFlag = true;
    }

    interface EventMessage {
        void process();
    }

    private class BootStateChangeEvent implements EventMessage {

        private final ProcessState newState;

        BootStateChangeEvent(ProcessState newState) {
            this.newState = newState;
        }

        public void process() {
            processBootStateChange(newState);
        }
    }

    private class RestartCountResetTask implements Runnable {
        public void run() {
            synchronized(gpSync) {
                restartCountBin = 0;
            }
        }
    }
}
