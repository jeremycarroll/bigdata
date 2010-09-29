/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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

package com.bigdata.boot;

import com.bigdata.DataFinder;
import com.bigdata.util.config.LogUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/*
 * Test suite for most of the boot launcher components. The tests provided
 * in this suite is intended to exercise the boot launcher itself, the
 * BootAgent, and the BootManager.
 */
public class BootComponentTest extends Assert {
    private static String P_SEP    = System.getProperty("path.separator");
    private static String APP_HOME = System.getProperty("app.home");

    private Process bootProcess;

    private String  testName;
    private boolean testPassed;

    private static Logger logger;

    // NOTE: remove constructors and tearDown when/if the junit infrastructure
    //       is upgraded to a version that supports annotations and this test
    //       is changed so that it no longer has to extend TestCase.

    public BootComponentTest() {
        initAll();
    }

    @After
    public void tearDown() throws Exception {
        cleanUp();
    }


//    @BeforeClass public static void initAll() {
    public static void initAll() {

        System.setProperty("log4j.configuration", DataFinder.bestPath("var/config/logging/log4j.properties") );
        logger = LogUtil.getLog4jLogger
                            ( (BootComponentTest.class).getName() );
    }

    // Kills any lingering processes
//    @After public void cleanUp() {
    public void cleanUp() {

        if(bootProcess != null) {
            logger.log(Level.DEBUG, 
                       "Destroying the boot launcher on test completion");
            bootProcess.destroy();
            try {
                bootProcess.waitFor();
            } catch(InterruptedException e) { /* swallow */ }
            this.bootProcess = null;
        }
        if(testName != null) {
            String prefix = (testPassed ? "PASSED: " : "FAILED: ");
            logger.log(Level.INFO, prefix+testName);
            logger.log(Level.INFO,
                       "--------------------------------------------------");
        } else {
            logger.log(Level.INFO, " ");
        }
    }

    private void startBootLauncher(String configFileName) throws IOException {

        // NOTE: below, the classes dir is used when running tests from ant,
        //       and the bin dir is used when running tests from eclipse.

        String javaCmd = "java";
        String cpArg   = "-cp";
        String cpVal   = DataFinder.bestPath("lib/bigdata-core.jar")
                         +P_SEP+
                         DataFinder.bestPath("testing/lib-test/bigdata-core-tests.jar")
                         +P_SEP+
                         DataFinder.bestPath("target/classes")
                         +P_SEP+
                         "bin";//ant-build/classes for ant, bin for eclipse
        String logProp = "-Djava.util.logging.config.file="
                         + DataFinder.bestPath("testing/data/com/bigdata/boot/config/launcher-logging.properties");
        String mainStr = "com.bigdata.boot.launcher.LauncherMain";
        String cfgStr  = DataFinder.bestPath("testing/data/com/bigdata/boot/config/" + configFileName);

        String[] execCmd = 
            new String[] { javaCmd, cpArg, cpVal, logProp, mainStr, cfgStr };

        File launcherWorkingDir;
        if (APP_HOME != null){
            launcherWorkingDir = new File(APP_HOME);//provided 'root'             
        } else {
            launcherWorkingDir = new File("");//same as this app
        }

        this.bootProcess = new ProcessBuilder().directory(launcherWorkingDir).command( execCmd ).start();

        logger.warn( "WORKING DIR FOR PROC: " + launcherWorkingDir.getAbsolutePath() );

        // Copy output boot launcher output to the current output location
        new StreamCopierThread
            (bootProcess.getInputStream(), System.out).start();
        new StreamCopierThread
            (bootProcess.getErrorStream(), System.err).start();
    }

    private BootManager getBootManager() throws InterruptedException {
        for(int i=0; i<12; i++) {
            try {
                return new BootManager();
            } catch(IOException e) {
                Thread.sleep(250);
            }
        }
        fail("FAILURE: unable to connect to boot launcher");
        return null;
    }

    // Test the BootManager API

//    @Test(timeout=3000, expected=IOException.class)
    @Test(timeout=3000)
    public void testBootManager_noConnect() throws Exception {
        try {
            BootManager bootMgr = new BootManager();
        } catch(IOException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    @Test(timeout=5000)
    public void testBootManager_getProcessTags() throws Exception {
        testName = "testBootManager_getProcessTags";
        testPassed = false;

        startBootLauncher("BootComponentTest_0boot.xml");

        // Connect a BootManager to the launcher
        BootManager bootMgr = getBootManager();

        // Get a list of process tags
        List<String> processTags = bootMgr.getProcessTags();
        Collections.sort(processTags);

        assertEquals(8, processTags.size());
        assertEquals("idleProc1", processTags.get(0));
        assertEquals("idleProc2", processTags.get(1));
        assertEquals("idleProc3", processTags.get(2));
        assertEquals("idleProcDoesntStop", processTags.get(3));
        assertEquals("idleProcFails6sec", processTags.get(4));
        assertEquals("initFailsProc", processTags.get(5));
        assertEquals("selfShutdownProc", processTags.get(6));
        assertEquals("selfShutdownProcFreezes", processTags.get(7));

        testPassed = true;
    }

    @Test(timeout=5000)
    public void testBootManager_getState() throws Exception {

        testName = "testBootManager_getState";
        testPassed = false;

        startBootLauncher("BootComponentTest_0boot.xml");

        // Connect a BootManager to the launcher
        BootManager bootMgr = getBootManager();

        // Get a list of process tags
        List<String> processTags = bootMgr.getProcessTags();

        assertFalse("Process tag list is empty", processTags.isEmpty());

        // Get status of each process tag and verify they're all STOPPED
        for(String tag : processTags) {
            ProcessState state = bootMgr.getState(tag);
            assertEquals("process state not STOPPED [tag='"+tag+"']",
                         ProcessState.STOPPED, state);
        }
        testPassed = true;
    }

//    @Test(timeout=5000, expected=IllegalArgumentException.class)
    @Test(timeout=5000)
    public void testBootManager_getState_noSuchProcess() throws Exception {

        testName = "testBootManager_getState_noSuchProcess";
        testPassed = false;

        startBootLauncher("BootComponentTest_0boot.xml");

        // Connect a BootManager to the launcher
        BootManager bootMgr = getBootManager();

        // Try to get the state of a process that doesn't exist
        try {
            bootMgr.getState("nonexistantProcess");
        } catch(IllegalArgumentException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

//    @Test(timeout=5000, expected=IllegalArgumentException.class)
    @Test(timeout=5000)
    public void testBootManager_startProcess_noSuchProcess() throws Exception {

        testName = "testBootManager_startProcess_noSuchProcess";
        testPassed = false;

        startBootLauncher("BootComponentTest_0boot.xml");

        // Connect a BootManager to the launcher
        BootManager bootMgr = getBootManager();

        // Try to start a process that doesn't exist
        try {
            bootMgr.startProcess("nonexistantProcess");
        } catch(IllegalArgumentException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

//    @Test(timeout=5000, expected=IllegalArgumentException.class)
    @Test(timeout=5000)
    public void testBootManager_stopProcess_noSuchProcess() throws Exception {

        testName = "testBootManager_stopProcess_noSuchProcess";
        testPassed = false;

        startBootLauncher("BootComponentTest_0boot.xml");

        // Connect a BootManager to the launcher
        BootManager bootMgr = getBootManager();

        // Try to stop a process that doesn't exist
        try {
            bootMgr.stopProcess("nonexistantProcess");
        } catch(IllegalArgumentException e) {
            testPassed = true;
//uncomment when @TEST(expected) works
//            throw e;
        }
    }

    // Test boot launcher starting/stopping

    // Start and stop an empty launcher
    @Test(timeout=10000)
    public void testStartAndStopEmptyLauncher() throws Exception {

        testName = "testStartAndStopEmptyLauncher";
        testPassed = false;

        startBootLauncher("BootComponentTest_0boot.xml");

        // Connect a BootManager to the launcher
        BootManager bootMgr = getBootManager();

        Thread.sleep(2000);

        // Stop the launcher through the BootManager
        bootMgr.stopLauncher();

        // Wait for the launcher to exit
        int exitCode = bootProcess.waitFor();
        assertEquals("failure exit code from boot launcher", 0, exitCode);

        this.bootProcess = null;

        testPassed = true;
    }

    // Start a launcher with 1 boot process that succeeds in starting
    @Test(timeout=6000)
    public void testStartLauncher1Process() throws Exception {

        testName = "testStartLauncherProcess";
        testPassed = false;

        StateSequenceListener listener =
            new StartTransitionListener("idleProc1");

        startBootLauncher("BootComponentTest_1boot.xml");

        // Connect a BootManager to the launcher and register a listener
        BootManager bootMgr = getBootManager();
        bootMgr.registerListener(listener);

        // The listener verifies state transitions and reports back
        String failureReason = listener.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        testPassed = true;
    }

    // Start a launcher with 1 boot process that fails to start
    @Test(timeout=6000)
    public void testStartLauncher1Process1Fails() throws Exception {

        testName = "testStartLauncher1ProcessFails";
        testPassed = false;

        StateSequenceListener listener =
            new StartFailureTransitionListener("initFailsProc");

        startBootLauncher("BootComponentTest_1boot_1fail.xml");

        // Connect a BootManager to the launcher and register a listener
        BootManager bootMgr = getBootManager();
        bootMgr.registerListener(listener);

        // The listener verifies the state transitions and reports back
        String failureReason = listener.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        // The boot launcher should exit since a boot process failed to start
        int exitCode = bootProcess.waitFor();
        assertTrue( "failure exit code from boot launcher", (exitCode != 0) );

        this.bootProcess = null;

        testPassed = true;
    }

    // Start a launcher with 3 boot processes that all succeed in starting
    @Test(timeout=12000)
    public void testStartLauncher3Processes() throws Exception {

        testName = "testStartLauncher3Processes";
        testPassed = false;

        StateSequenceListener listener1 =
            new StartTransitionListener("idleProc1");
        StateSequenceListener listener2 =
            new StartTransitionListener("idleProc2");
        StateSequenceListener listener3 =
            new StartTransitionListener("idleProc3");

        startBootLauncher("BootComponentTest_3boot.xml");

        // Connect a BootManager to the launcher and register listeners
        // for each process
        BootManager bootMgr = getBootManager();
        bootMgr.registerListener(listener1);
        bootMgr.registerListener(listener2);
        bootMgr.registerListener(listener3);

        // The listeners verify the state transitions and report back
        String failureReason = listener1.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        failureReason = listener2.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        failureReason = listener3.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        testPassed = true;
    }

    // Start a launcher with 3 boot processes, 1 of which fails to start
    @Test(timeout=12000)
    public void testStartLauncher3Processes1Fails() throws Exception {

        testName = "testStartLauncher3Processes1Fails";
        testPassed = false;

        StateSequenceListener listener1 =
            new StartTransitionListener("idleProc1");
        StateSequenceListener listener2 =
            new StartFailureTransitionListener("initFailsProc");
        StateSequenceListener listener3 =
            new StartTransitionListener("idleProc3");

        startBootLauncher("BootComponentTest_3boot_1fail.xml");

        // Connect a BootManager to the launcher and register listeners
        // for each process
        BootManager bootMgr = getBootManager();
        bootMgr.registerListener(listener1);
        bootMgr.registerListener(listener2);
        bootMgr.registerListener(listener3);

        // The listeners verify the state transitions and report back
        String failureReason = listener1.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        failureReason = listener2.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        // idleProc3 should never get started because the second process
        // failed to start

        // The boot launcher should exit since a boot process failed to start
        int exitCode = bootProcess.waitFor();
        assertTrue( "exit code from boot launcher does not indicate "
                    +"failure as expected", (exitCode != 0) );

        this.bootProcess = null;

        testPassed = true;
    }

    // Stop a launcher with 3 running processes
    @Test(timeout=12000)
    public void testStopLauncher3Processes() throws Exception {

        testName = "testStopLauncher3Processes";
        testPassed = false;

        StateSequenceListener listener1 =
            new StartTransitionListener("idleProc1");
        StateSequenceListener listener2 =
            new StartTransitionListener("idleProc2");
        StateSequenceListener listener3 =
            new StartTransitionListener("idleProc3");

        startBootLauncher("BootComponentTest_3boot.xml");

        // Connect a BootManager to the launcher and register listeners
        // for each process
        BootManager bootMgr = getBootManager();
        bootMgr.registerListener(listener1);
        bootMgr.registerListener(listener2);
        bootMgr.registerListener(listener3);

        // The listeners verify the state transitions and report back
        String failureReason = listener1.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        failureReason = listener2.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        failureReason = listener3.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        // All 3 processes are started. Register new listeners to verify
        // they all get shut down gracefully.
        bootMgr.unregisterListener(listener1);
        bootMgr.unregisterListener(listener2);
        bootMgr.unregisterListener(listener3);

        listener1 = new StateSequenceListener("idleProc1",
           new ProcessState[] { ProcessState.STOPPING, ProcessState.STOPPED });

        listener2 = new StateSequenceListener("idleProc2",
           new ProcessState[] { ProcessState.STOPPING, ProcessState.STOPPED });

        listener3 = new StateSequenceListener("idleProc3",
           new ProcessState[] { ProcessState.STOPPING, ProcessState.STOPPED });

        bootMgr.registerListener(listener1);
        bootMgr.registerListener(listener2);
        bootMgr.registerListener(listener3);

        // Shut down the launcher and wait for the launcher to exit
        bootMgr.stopLauncher();

        failureReason = listener1.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        failureReason = listener2.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        failureReason = listener3.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        int exitCode = bootProcess.waitFor();
        assertEquals("failure exit code from boot launcher", 0, exitCode);

        this.bootProcess = null;

        testPassed = true;
    }

    // Start a launcher with 3 boot processes, 1 of which eventually crashes
    // causing the boot launcher to shut down
    @Test(timeout=20000)
    public void testBootProcessCrash() throws Exception {

        testName = "testBootProcessCrash";
        testPassed = false;

        StateSequenceListener listener1 =
            new StartTransitionListener("idleProc1");
        StateSequenceListener listener2 =
            new StartTransitionListener("idleProcFails6sec");
        StateSequenceListener listener3 =
            new StartTransitionListener("idleProc3");

        startBootLauncher("BootComponentTest_3boot_1fail_later.xml");

        // Connect a BootManager to the launcher and register listeners
        // for each process
        BootManager bootMgr = getBootManager();
        bootMgr.registerListener(listener1);
        bootMgr.registerListener(listener2);
        bootMgr.registerListener(listener3);

        // The listeners verify the state transitions and report back
        String failureReason = listener1.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        failureReason = listener2.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        failureReason = listener3.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        // The boot launcher should exit since a boot process failed
        int exitCode = bootProcess.waitFor();
        assertTrue( "exit code from boot launcher does not indicate "
                    +"failure as expected", (exitCode != 0) );

        this.bootProcess = null;

        testPassed = true;
    }


    // Test state transition processing

    // Start a process that fails to start
    @Test(timeout=10000)
    public void testStartProcessFailToStart() throws Exception {

        testName = "testStartProcessFailToStart";
        testPassed = false;

        // Start an empty launcher
        startBootLauncher("BootComponentTest_0boot.xml");
        BootManager bootMgr = getBootManager();

        // Register a listener
        StateSequenceListener listener = 
            new StateSequenceListener
                ( "initFailsProc", 
                  new ProcessState[] {ProcessState.STARTING,
                                     ProcessState.FAILED } );
        bootMgr.registerListener(listener);

        // Start the process and wait for it
        bootMgr.startProcess("initFailsProc");

        String failureReason = listener.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        testPassed = true;
    }

    // Start a process that succeeds in starting
    @Test(timeout=10000)
    public void testStartProcessSuccess() throws Exception {

        testName = "testStartProcessSuccess";
        testPassed = false;

        // Start an empty launcher
        startBootLauncher("BootComponentTest_0boot.xml");
        BootManager bootMgr = getBootManager();

        // Register a listener
        StateSequenceListener listener = 
            new StateSequenceListener
                ( "idleProc1",
                  new ProcessState[] { ProcessState.STARTING,
                                       ProcessState.RUNNING } );
        bootMgr.registerListener(listener);

        // Start the process and wait for it
        bootMgr.startProcess("idleProc1");

        String failureReason = listener.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        testPassed = true;
    }

    // Stop a process
    @Test(timeout=10000)
    public void testStopProcessSuccess() throws Exception {

        testName = "testStopProcessSuccess";
        testPassed = false;

        // Start an empty launcher
        startBootLauncher("BootComponentTest_0boot.xml");
        BootManager bootMgr = getBootManager();

        // Register a listener
        StateSequenceListener listener =
            new StateSequenceListener
                ( "idleProc1",
                  new ProcessState[] { ProcessState.STARTING,
                                       ProcessState.RUNNING } );
        bootMgr.registerListener(listener);

        // Start the process and wait for it
        bootMgr.startProcess("idleProc1");

        String failureReason = listener.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        // Re-register a new listener to listen for the shutdown
        bootMgr.unregisterListener(listener);
        listener = new StateSequenceListener
                    ("idleProc1", new ProcessState[] { ProcessState.STOPPING,
                                                       ProcessState.STOPPED });
        bootMgr.registerListener(listener);

        // Stop the process and wait for it to complete
        bootMgr.stopProcess("idleProc1");

        failureReason = listener.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        testPassed = true;
    }

    // Stop a process and it exceeds the stop timelimit
    @Test(timeout=10000)
    public void testStopProcessExceedTimelimit() throws Exception {

        testName = "testStopProcessExceedTimelimit";
        testPassed = false;

        // Start an empty launcher
        startBootLauncher("BootComponentTest_0boot.xml");
        BootManager bootMgr = getBootManager();

        // Register a listener
        StateSequenceListener listener =
            new StateSequenceListener
                ( "idleProcDoesntStop",
                  new ProcessState[] { ProcessState.STARTING,
                                       ProcessState.RUNNING } );
        bootMgr.registerListener(listener);

        // Start the process and wait for it
        bootMgr.startProcess("idleProcDoesntStop");

        String failureReason = listener.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        // Re-register a new listener to listen for the shutdown
        bootMgr.unregisterListener(listener);
        listener = new StateSequenceListener
                       ( "idleProcDoesntStop",
                         new ProcessState[] { ProcessState.STOPPING,
                                              ProcessState.STOPPED } );
        bootMgr.registerListener(listener);

        // Stop the process and wait for it to complete. The time it takes
        // to stop should be (approximately) the stopTimeout for the process
        long beforeTime = System.currentTimeMillis();
        bootMgr.stopProcess("idleProcDoesntStop");

        failureReason = listener.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        long deltaTime = System.currentTimeMillis() - beforeTime;
        if( (deltaTime < (5000-250)) || (deltaTime > (5000+2000)) ) {
            fail("process stop time out of range "
                 +"[stop time="+deltaTime+" ms]");
        }

        testPassed = true;
    }

    // Start a process that stops itself
    @Test(timeout=8000)
    public void testSelfStopProcessSuccess() throws Exception {

        testName = "testSelfStopProcessSuccess";
        testPassed = false;

        // Start an empty launcher
        startBootLauncher("BootComponentTest_0boot.xml");
        BootManager bootMgr = getBootManager();

        // Register a listener
        StateSequenceListener listener =
            new StateSequenceListener
                ( "selfShutdownProc", 
                  new ProcessState[] { ProcessState.STARTING,
                                       ProcessState.RUNNING,
                                       ProcessState.STOPPING,
                                       ProcessState.STOPPED } );
        bootMgr.registerListener(listener);

        // Start the process and wait for all expected state transitions
        bootMgr.startProcess("selfShutdownProc");

        String failureReason = listener.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        testPassed = true;
    }

    // Start a process that stops itself but exceeds the stop time limit
    @Test(timeout=20000)
    public void testSelfStopProcessExceedTimelimit() throws Exception {

        testName = "testSelfStopProcessExceedTimelimit";
        testPassed = false;

        // Start an empty launcher
        startBootLauncher("BootComponentTest_0boot.xml");
        BootManager bootMgr = getBootManager();

        // Register a listener
        StateSequenceListener listener =
            new StateSequenceListener
                ( "selfShutdownProcFreezes",
                  new ProcessState[] { ProcessState.STARTING,
                                       ProcessState.RUNNING,
                                       ProcessState.STOPPING,
                                       ProcessState.STOPPED } );
        bootMgr.registerListener(listener);

        // Start the process and wait for all the expected state transitions.
        // The time to start and stop should be ~(stopTimeout + 1s)
        long beforeTime = System.currentTimeMillis();
        bootMgr.startProcess("selfShutdownProcFreezes");

        String failureReason = listener.waitUntilDone();
        if(failureReason != null) fail(failureReason);

        long deltaTime = System.currentTimeMillis() - beforeTime;
        if( (deltaTime < (8000-250)) || (deltaTime > (8000+2000)) ) {
            fail("process stop time out of range "
                 +"[stop time="+deltaTime+" ms]");
        }

        testPassed = true;
    }

    static class StreamCopierThread extends Thread {

        private final InputStream input;
        private final OutputStream output;

        StreamCopierThread(InputStream input, OutputStream output) {
            setDaemon(true);
            this.input = input;
            this.output = output;
        }

        @Override
        public void run() {
            byte[] buf = new byte[256];
            try {
                // Transfer from input to output forever
                while(true) {
                    int len = input.read(buf);
                    if(len < 0) break;
                    output.write(buf, 0, len);
                }
            } catch(IOException e) { }
        }
    }

    static class StateSequenceListener implements ProcessEventListener {

        protected final String ptag;
        protected final ProcessState[] sequence;
        protected int seqIndex;
        protected boolean done;
        protected String failureReason;

        StateSequenceListener(String ptag, ProcessState[] sequence) {
            this.ptag = ptag;
            this.sequence = sequence;
        }

        public void processStateChangeEvent(ProcessStateChangeEvent event) {

            if( !event.tag.equals(ptag) || done ) return;

            // Check the event against the previous and expected values
            if( (seqIndex > 0) && (event.prevState != sequence[seqIndex-1]) ) {

                recordFailure("unexpected previous state - "
                              +"[state expected="+sequence[seqIndex-1]
                              +", state received="+event.prevState+"]");
            } else if(event.currState != sequence[seqIndex]) {

                recordFailure("unexpected new state - "
                              +"[state expected="+sequence[seqIndex-1]
                              +", state received="+event.prevState+"]");
            }
            seqIndex++;
            if(seqIndex == sequence.length) recordSuccess();
        }

        public synchronized String waitUntilDone() 
            throws InterruptedException
        {
            while( !done ) {
                this.wait();
            }
            return failureReason;
        }

        protected synchronized void recordFailure(String reason) {
            if( !done ) {
                this.failureReason = ptag + ": " + reason;
                this.done = true;
                this.notifyAll();
            }
        }

        protected synchronized void recordSuccess() {
            if( !done ) {
                this.done = true;
                this.notifyAll();
            }
        }
    }

    static class StartTransitionListener extends StateSequenceListener {

        private ProcessState lastState;

        StartTransitionListener(String ptag) {
            super(ptag, null);
        }

        @Override
        public void processStateChangeEvent(ProcessStateChangeEvent event) {

            if( !event.tag.equals(ptag) || done ) return;

            // Verify the event's previous state matches the last state
            if( (lastState != null) && (event.prevState != lastState) ) {
                recordFailure("unexpected previous state - "
                              +"[state expected="+lastState
                              +", state received="+event.prevState+"]");
            }

            this.lastState = event.currState;

            // Check for a valid transition
            if(event.prevState == ProcessState.STOPPED) {
                if(event.currState != ProcessState.STARTING) {
                    recordFailure("unexpected new state - "
                                  +"[state expected=STARTING"
                                  +", state received="+event.currState+"]");
                }
            } else if(event.prevState == ProcessState.STARTING) {
                if(event.currState == ProcessState.RUNNING) {
                    recordSuccess();
                } else {
                    recordFailure("unexpected new state - "
                                  +"[state expected=RUNNING"
                                  +", state received="+event.currState+"]");
                }
            } else {
                recordFailure("unexpected process transition - "
                              +"[previous state="+event.prevState
                              +" --> new state="+event.currState+"]");
            }
        }
    }

    static class StartFailureTransitionListener extends StateSequenceListener {

        private ProcessState lastState;

        StartFailureTransitionListener(String ptag) {
            super(ptag, null);
        }

        @Override
        public void processStateChangeEvent(ProcessStateChangeEvent event) {

            if( !event.tag.equals(ptag) || done ) return;

            // Verify the event's previous state matches the last state
            if( (lastState != null) && (event.prevState != lastState) ) {
                recordFailure
                    ("previous state in received event does match "
                     +"last known state - [last known state=lastState"
                     +", previous state from event="+event.prevState+"]");
            }

            this.lastState = event.currState;

            // Check for a valid transition
            if(event.prevState == ProcessState.STOPPED) {
                if(event.currState != ProcessState.STARTING) {
                    recordFailure("unexpected new state - "
                                  +"[state expected=STARTING"
                                  +", state received="+event.currState+"]");
                }
            } else if(event.prevState == ProcessState.STARTING) {
                if(event.currState == ProcessState.FAILED) {
                    recordSuccess();
                } else {
                    recordFailure("unexpected new state - "
                                  +"[state expected=FAILED"
                                  +", state received="+event.currState+"]");
                }
            } else {
                recordFailure("unexpected process transition - "
                              +"[previous state="+event.prevState
                              +" --> new state="+event.currState+"]");
            }
        }
    }
}
