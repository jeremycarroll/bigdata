/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
/*
 * Created on Feb 8, 2012
 */

package com.bigdata.bop.engine;

import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.Instrument;

/**
 * {@link QueryEngine} performance counters.
 * 
 * @author thompsonbry
 */
public class QueryEngineCounters implements ICounterSetAccess {

    /**
     * The #of queries which have been executed (set on completion).
     */
    final CAT queryStartCount = new CAT();

    /**
     * The #of queries which have been executed (set on completion).
     */
    final CAT queryDoneCount = new CAT();

    /**
     * The #of instances of the query which terminated abnormally.
     */
    final CAT queryErrorCount = new CAT();

    /**
     * The total elapsed time (millis) for evaluation queries. This is the wall
     * clock time per query. The aggregated wall clock time per query will sum
     * to greater than the elapsed wall clock time in any interval where there
     * is more than one query running concurrently.
     */
    final CAT elapsedMillis = new CAT();

    /*
     * Lower level counters dealing with the work queues and executing chunk
     * tasks.
     */

    // /**
    // * The #of non-empty work queues.
    // */
    // final CAT workQueueCount = new CAT();

    /**
     * The #of work queues which are currently blocked.
     */
    final CAT blockedWorkQueueCount = new CAT();

    /**
     * The #of times that a work queue has blocked.
     */
    final CAT blockedWorkQueueRunningTotal = new CAT();

    /**
     * The #of active operator evaluation tasks (chunk tasks).
     */
    final CAT operatorActiveCount = new CAT();

    /**
     * The #of operator evaluation tasks (chunk tasks) which have started.
     */
    final CAT operatorStartCount = new CAT();

    /**
     * The #of operator evaluation tasks (chunk tasks) which have ended.
     */
    final CAT operatorHaltCount = new CAT();

    public CounterSet getCounters() {

        final CounterSet root = new CounterSet();

        // #of queries started on this server.
        root.addCounter("queryStartCount", new Instrument<Long>() {
            public void sample() {
                setValue(queryStartCount.get());
            }
        });

        // #of queries retired on this server.
        root.addCounter("queryDoneCount", new Instrument<Long>() {
            public void sample() {
                setValue(queryDoneCount.get());
            }
        });

        // #of queries with abnormal termination on this server.
        root.addCounter("queryErrorCount", new Instrument<Long>() {
            public void sample() {
                setValue(queryErrorCount.get());
            }
        });

        // #of queries retired per second on this server.
        root.addCounter("queriesPerSecond", new Instrument<Double>() {
            public void sample() {
                final long ms = elapsedMillis.get();
                final long n = queryDoneCount.get();
                // compute throughput, normalized to q/s := (q*1000)/ms.
                final double d = ms == 0 ? 0d : ((1000d * n) / ms);
                setValue(d);
            }
        });

        // // #of non-empty work queues.
        // root.addCounter("workQueueCount", new Instrument<Long>() {
        // public void sample() {
        // setValue(workQueueCount.get());
        // }
        // });

        // #of blocked work queues.
        root.addCounter("blockedWorkQueueCount", new Instrument<Long>() {
            public void sample() {
                setValue(blockedWorkQueueCount.get());
            }
        });

        // #of times that a work queue has blocked.
        root.addCounter("blockedWorkQueueRunningTotal", new Instrument<Long>() {
            public void sample() {
                setValue(blockedWorkQueueRunningTotal.get());
            }
        });

        // #of concurrently executing operator tasks.
        root.addCounter("operatorActiveCount", new Instrument<Long>() {
            public void sample() {
                setValue(operatorActiveCount.get());
            }
        });

        // #of operator evaluation tasks which have started.
        root.addCounter("operatorStartCount", new Instrument<Long>() {
            public void sample() {
                setValue(operatorStartCount.get());
            }
        });

        // #of operator evaluation tasks which have ended.
        root.addCounter("operatorHaltCount", new Instrument<Long>() {
            public void sample() {
                setValue(operatorHaltCount.get());
            }
        });

        return root;

    }

}