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
package com.bigdata.boot;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class BootTool {
    public static void main(String args[]) {
        try {
            BootTool app = new BootTool();
            int rc = app.run( new LinkedList<String>(Arrays.asList(args)) );
            System.exit(rc);
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(63);
        }
    }

    private void usage() {
        System.err.println(
"Usage: BootTool [--port=<n>] <command> [args...]\n" +
"\n" +
"Commands:\n" +
"    start [--wait] <processtag> - Starts a process. The optional --wait\n" +
"                                  argument will cause this tool to wait\n" +
"                                  for the process to successfully start or\n" +
"                                  fail to start.\n" +
"    stop [--wait] <processtag>  - Stops a process. The optional --wait\n" +
"                                  argument will cause this tool to wait\n" +
"                                  for the process to exit.\n" +
"    status [processtag] ...     - Gets the status of specified processes.\n" +
"    stoplauncher                - Shuts down the boot launcher.\n");
    }

    private int run(List<String> args) throws Exception {
        // Process command line options
        int port = -1;
        while( !args.isEmpty() ) {
            if( args.get(0).startsWith("--port=") ) {
                port = Integer.parseInt(args.get(0).substring(7));
                args.remove(0);
            } else if( args.get(0).equals("--help") ) {
                usage();
                return 0;
            } else {
                break;
            }
        }

        if( args.isEmpty() ) {
            System.err.println("ERROR: insufficient arguments\n");
            usage();
            return 1;
        }

        BootManager bootMgr = (port > 0) ? 
                                  new BootManager(port) : new BootManager();

        if( "start".equalsIgnoreCase(args.get(0)) ) {
            return doStart(bootMgr, args.subList(1, args.size()));
        } else if( "stop".equalsIgnoreCase(args.get(0)) ) {
            return doStop(bootMgr, args.subList(1, args.size()));
        } else if( "stoplauncher".equalsIgnoreCase(args.get(0)) ) {
            bootMgr.stopLauncher();
        } else if( "status".equalsIgnoreCase(args.get(0)) ) {
            return doStatus(bootMgr, args.subList(1, args.size()));
        } else {
            System.err.println("ERROR: Unknown command '"+args.get(0)+"'\n");
            usage();
            return 1;
        }
        return 0;
    }

    private int doStart(BootManager bootMgr, List<String> args)
                    throws Exception
    {
        boolean waitFor = false;
        if( !args.isEmpty() && "--wait".equals(args.get(0)) ) {
            waitFor = true;
            args.remove(0);
        }
        if( args.isEmpty() ) {
            System.err.println("ERROR: insufficient arguments\n");
            usage();
            return 1;
        }
        String ptag = args.get(0);

        StartListener startListener = new StartListener(ptag);
        bootMgr.registerListener(startListener);
        startListener.setInitialState(bootMgr.getState(ptag));
        bootMgr.startProcess(ptag);
        if(waitFor) {
            ProcessState finalState = startListener.waitForChange();
            if(finalState == ProcessState.RUNNING) {
                System.out.println(ptag+" RUNNING");
            } else {
                System.out.println(ptag+" " + finalState.toString());
                return 1;
            }
        }
        bootMgr.unregisterListener(startListener);
        return 0;
    }

    private int doStop(BootManager bootMgr, List<String> args)
                    throws Exception
    {
        boolean waitFor = false;
        if( !args.isEmpty() && "--wait".equals(args.get(0)) ) {
            waitFor = true;
            args.remove(0);
        }
        if(args.isEmpty()) {
            System.err.println("ERROR: insufficient arguments\n");
            usage();
            return 1;
        }
        String ptag = args.get(0);

        StopListener stopListener = new StopListener(ptag);
        bootMgr.registerListener(stopListener);
        stopListener.setInitialState(bootMgr.getState(ptag));
        bootMgr.stopProcess(ptag);
        if(waitFor) {
            ProcessState finalState = stopListener.waitForChange();
            if( finalState == ProcessState.STOPPED ) {
                System.out.println(ptag+" STOPPED");
            } else {
                System.out.println(ptag+" " + finalState.toString());
            }
        }
        bootMgr.unregisterListener(stopListener);
        return 0;
    }

    private int doStatus(BootManager bootMgr, List<String> args)
                    throws Exception
    {
        List<String> processTags = args;

        if( args.isEmpty() ) processTags = bootMgr.getProcessTags();

        for(String tag : processTags) {
            ProcessState state = bootMgr.getState(tag);
            System.out.println(tag + " is " + state.toString());
        }
        return 0;
    }

    static class StateListener implements ProcessEventListener {
        protected final String ptag;
        protected final ProcessState desiredState;
        protected ProcessState finalState;

        public StateListener(String ptag, ProcessState endState) {
            this.ptag = ptag;
            this.desiredState = endState;
        }

        public synchronized void processStateChangeEvent
                                     (ProcessStateChangeEvent event)
        {
            if( ptag.equals(event.tag) && (finalState == null) ) {
                if( (event.currState == desiredState) ||
                    (event.currState == ProcessState.FAILED) )
                {
                    this.finalState = event.currState;
                    this.notifyAll();
                }
            }
        }

        public synchronized ProcessState waitForChange()
                                             throws InterruptedException
        {
            while(finalState == null) {
                this.wait();
            }
            return finalState;
        }
    }

    static class StartListener extends StateListener {
        StartListener(String ptag) {
            super(ptag, ProcessState.RUNNING);
        }

        public synchronized void setInitialState(ProcessState state) {
            if( (finalState == null) && (state == ProcessState.RUNNING) ) {
                // Already running
                this.finalState = state;
                this.notifyAll();
            }
        }
    }

    static class StopListener extends StateListener {
        StopListener(String ptag) {
            super(ptag, ProcessState.STOPPED);
        }

        public synchronized void setInitialState(ProcessState state) {
            if( (finalState == null) && ((state == ProcessState.STOPPED) ||
                (state == ProcessState.FAILED)) )
            {
                // Already stopped
                this.finalState = state;
                this.notifyAll();
            }
        }
    }
}
