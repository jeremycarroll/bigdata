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
package com.bigdata.boot.launcher;

import com.bigdata.boot.MessageTransport;
import com.bigdata.boot.ProcessState;
import com.bigdata.boot.ProcessStateChangeEvent;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.xml.sax.SAXException;

public class LauncherMain {

    protected static final Logger logger =
                           Logger.getLogger("com.bigdata.boot.launcher");

    /** Queue for events to be processed by the main application loop. */
    private final BlockingQueue<Object> eventQueue =
                                        new LinkedBlockingQueue<Object>();

    /** Ordered map whose elements associate process tags to their
     *  descriptors.
     */
    private final Map<String,ProcessDescriptor> processMap =
    new TreeMap<String,ProcessDescriptor>();

    /** Timer used to execute TimerTasks. */
    private final Timer timer = new Timer(true);

    /**
     * List whose elements are the processes to start when the launcher
     * itself starts. This list is ordered by the order defined in the
     * configuration file; which is the order in which the processes
     * will be started.
     */
    private List<String> bootProcesses;

    /** The properties used to configure the boot launcher. */
    private ConfigurationPropertyMap globalProperties;

    /**
     * Holds the current state of the boot launcher. Note that although
     * the value of this field can be read by multiple threads, only
     * the main application thread writes to this field.
     */
    private ProcessState launcherState;

    /**
     * Command listener that accepts connections and receives commands in
     * the background.
     */
    private ControlCommandListener commandListener;

    public static void main(String[] args) {
        if(args.length < 1) {
            System.err.println("no configuration file provided");
            System.exit(1);
        }

        try {
            LauncherMain app = new LauncherMain(args[0]);
            int rc = app.run();
            System.exit(rc);
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public LauncherMain(String configFileName) throws IOException {

        readConfigFile(new File(configFileName));

        int commandListenPort = 
            Integer.parseInt
                (globalProperties.getProperties().get("boot.port"));

        // Verify all boot processes are defined
        for(String tag : bootProcesses) {
            if(processMap.get(tag) == null) {
                throw new IllegalStateException
                              ("boot process does not exist [tag='"+tag+"']");
            }
        }

        changeLauncherState(ProcessState.STARTING);

        logger.log(Level.FINE, "creating command listener");
        this.commandListener = new ControlCommandListener
                                       (this, commandListenPort);
    }

    /** Returns boolean value indicating if the launcher is shutting down. */
    protected boolean isShuttingDown() {
        return ( (launcherState == ProcessState.STOPPING) ||
                 (launcherState == ProcessState.STOPPED) );
    }

    protected ProcessDescriptor findProcessDescriptor(String ptag) {
        return processMap.get(ptag);
    }

    protected List<String> getProcessTagList() {
        return new ArrayList<String>(processMap.keySet());
    }

    /**
     * Signals that a process state change has occurred. This method simply
     * enqueues a notification to the main thread loop so that it can notify
     * interested parties. This method can be called from any thread.
     */
    protected void signalProcessStateChange(String       ptag,
                                            ProcessState prevState,
                                            ProcessState currState)
    {
        eventQueue.add(new ProcessStateChangeEvent(ptag,prevState,currState));
    }

    /**
     * Signals that the launcher should be shut down. This method simply
     * enqueues a notification to the main thread loop so that it can perform
     * the shutdown. This method can be called from any thread.
     */
    protected void signalShutdown() {
        eventQueue.add(new ShutdownEvent());
    }

    /** Returns the timer used to execute TimerTasks. */
    protected Timer getTimer() {
        return timer;
    }

    protected int run() throws InterruptedException {
        logger.log(Level.INFO, "starting boot launcher");

// TODO: register a shutdown hook?

        // Start the first boot process
        if( !(bootProcesses.isEmpty()) ) {
            processMap.get(bootProcesses.get(0)).start();
        } else {
            // No boot processes to start
            changeLauncherState(ProcessState.RUNNING);
        }

        // Enter event processing loop
        int rc = 0;
        while(launcherState != ProcessState.STOPPED) {

            Object ev = eventQueue.take();

            if(ev instanceof ProcessStateChangeEvent) {

                // Notify all listeners
                ProcessStateChangeEvent pev = (ProcessStateChangeEvent)ev;
                commandListener.broadcastEvent
                                  ( Arrays.asList("STATECHANGE", pev.tag,
                                                  pev.prevState.toString(),
                                                  pev.currState.toString()) );

                if( (pev.currState == ProcessState.FAILED) &&
                    bootProcesses.contains(pev.tag) )
                {
                    logger.log(Level.SEVERE, "boot process failed "
                               +"[process='"+pev.tag+"'] - shutting down");
                    rc = 2;
                    beginShutdown();
                } else if(launcherState == ProcessState.STARTING) {
                    // Perform startup processing
                    kickLauncherStartup(pev);
                }

            } else if(ev instanceof ShutdownEvent) {

                beginShutdown();
            }

            // Check to see if shutdown is complete
            if(launcherState == ProcessState.STOPPING) {

                // All processes must be STOPPED or FAILED
                boolean allProcessesStopped = true;
                for(ProcessDescriptor pd : processMap.values()) {
                    ProcessState state = pd.getState();
                    if( (state != ProcessState.STOPPED) &&
                        (state != ProcessState.FAILED) )
                    {
                        // Process is still running
                        allProcessesStopped = false;
                        break;
                    }
                }

                if( allProcessesStopped && eventQueue.isEmpty() ) {
                    changeLauncherState(ProcessState.STOPPED);
                    logger.log(Level.INFO, "all processes have been stopped.");
                }
            }
        }
        logger.log(Level.INFO, "boot launcher is shutting down.");

        commandListener.terminate();
        return rc;
    }

    // Handles process state change events when the launcher is in the
    // STARTING state.
    private void kickLauncherStartup(ProcessStateChangeEvent event) {
        // Care about boot processes
        if(bootProcesses.contains(event.tag)) {
            if(event.currState == ProcessState.RUNNING) {
                // Start the next boot process
                int i = bootProcesses.indexOf(event.tag);
                if( (i >= 0) && (i < (bootProcesses.size() - 1)) ) {
                    String tag = bootProcesses.get(i+1);
                    processMap.get(tag).start();
                } else {
                    // No more boot processes to start
                    logger.log(Level.INFO, "all boot processes are running.");
                    changeLauncherState(ProcessState.RUNNING);
                }
            }
        }
    }

    // This method is only called from the main application thread
    private void beginShutdown() {
        if(launcherState != ProcessState.STOPPING) {
            changeLauncherState(ProcessState.STOPPING);

            // Signal all the processes to stop
            for(ProcessDescriptor pd : processMap.values()) {
                pd.stop(false);
            }
        }
    }

    private void changeLauncherState(ProcessState newState) {
        this.launcherState = newState;
// TODO: write the current state to a file for monitoring?
    }

    /** Creates a configuration containing the pre-defined properties */
    protected ConfigurationPropertyMap createBasePropertyMap() {
        ConfigurationPropertyMap base = new ConfigurationPropertyMap();

        String osName = System.getProperty("os.name").toLowerCase();
        if(osName.startsWith("windows")) {
            base.setProperty("os.name", "windows");
        } else {
            base.setProperty("os.name", osName.replaceAll("[ /]", ""));
        }
        base.setProperty("os.version", System.getProperty("os.version"));

        String arch = System.getProperty("os.arch").toLowerCase();
        if(arch.matches("^i?86$")) {
            base.setProperty("arch", "x86");
        } else if(arch.equals("amd64")) {
            base.setProperty("arch", "x86_64");
        } else {
            base.setProperty("arch", arch.replaceAll("[ /]", ""));
        }

        base.setProperty("/", System.getProperty("file.separator"));
        base.setProperty(":", System.getProperty("path.separator"));
        base.setProperty
            ("boot.port", Integer.toString(MessageTransport.DEFAULT_PORT));
        return base;
    }

    /** Reads and processes the contents of the configuration file. */
    protected void readConfigFile(File f) throws IOException {
        ConfigReader rdr;
        try {
            rdr = new ConfigReader(f, createBasePropertyMap());
        } catch(SAXException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

        // Create the service objects
        for(ProcessConfiguration config : rdr.getProcessList()) {
            ProcessDescriptor pd;

            // Create a new descriptor
            if("java".equals(config.cls)) {
                pd = new JavaProcessDescriptor(this, config);
            } else if("std".equals(config.cls)) {
                pd = new StdProcessDescriptor(this, config);
            } else if("hybrid".equals(config.cls)) {
                pd = new HybridProcessDescriptor(this, config);
            } else {
                throw new IllegalStateException("Process ['"+config.tag+"'] "
                                                +"has unknown class "
                                                +"['" + config.cls + "']");
            }
            processMap.put(config.tag, pd);
        }
        this.bootProcesses = rdr.getBootProcesses();
        this.globalProperties = rdr.getBootPropertyMap();
    }
}
