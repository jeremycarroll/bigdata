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

import java.util.concurrent.CountDownLatch;

public class SimulatedApp {

    public static void main(String args[]) {

        try {
            if(args.length < 1) {
                System.err.println("ERROR: insufficient arguments");
                System.exit(1);
            }
            String mode = args[0];

            if( mode.equalsIgnoreCase("crashBeforeBootAgent") ) {
                System.exit(63);
            }

            BootAgent bootAgent = BootAgent.getInstance();

            // Allow time for "initialization" to be performed
            Thread.sleep(1000);

            if( mode.equalsIgnoreCase("crashBeforeInitComplete") ) {
                System.exit(63);
            }
            bootAgent.initComplete();

            if( mode.equalsIgnoreCase("crashAfterInitComplete") ) {
                System.exit(63);
            }

            if( mode.equalsIgnoreCase("selfShutdown") ) {

                // Send 'shutting down' message, then actually shut down
                Thread.sleep(2000);
                bootAgent.shuttingDown();

            } else if( mode.equalsIgnoreCase("selfShutdownFreeze") ) {

                // Send 'shutting down' message, then then lock up
                Thread.sleep(2000);
                bootAgent.shuttingDown();
                while(true) { Thread.sleep(10000); }

            } else if( mode.equalsIgnoreCase("waitForShutdown") ) {

                // Register a listener and wait for the launcher to send
                // a command to this app telling it to shut down
                CountDownLatch latch = new CountDownLatch(1);
                bootAgent.registerListener(new ShutdownListener(latch));
                latch.await();

            } else if( mode.equalsIgnoreCase("ignoreShutdown") ) {

                // Do "work" forever, ignoring shutdown requests
                while(true) { Thread.sleep(10000); }

            } else if( mode.equalsIgnoreCase("crashAfter6Seconds") ) {

                // Do "work" for 6 seconds and then crash
                Thread.sleep(6000);
                System.exit(63);
            }

            System.exit(0);

        } catch(Throwable t) {
            t.printStackTrace();
            System.exit(255);
        }
    }

    static class ShutdownListener implements AgentListener {

        private final CountDownLatch latch;

        ShutdownListener(CountDownLatch latch) {
            this.latch = latch;
        }

        public void beginShutdown() {
            latch.countDown();
        }
    }
}
