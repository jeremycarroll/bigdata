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
/*
 * Created on Apr 16, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.util.LinkedHashMap;
import java.util.Map;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.resources.StaleLocatorException;

/**
 * 
 * @param <L>
 *            The generic type of the key used to lookup subtasks in the interal
 *            map.
 * @param <HS>
 *            The generic type of the subtask statistics.
 *            
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractMasterStats<L, HS extends AbstractSubtaskStats> {

    /**
     * The #of subtasks which have started.
     */
    public long subtaskStartCount = 0L;

    /**
     * The #of subtasks which have finished (either the buffer has been closed
     * and all buffered data has been flushed -or- the task was interrupted or
     * otherwise threw an exception).
     */
    public long subtaskEndCount = 0L;

    /**
     * The #of {@link StaleLocatorException}s that were handled.
     */
    public long staleLocatorCount = 0L;

    /** The #of chunks written on the {@link BlockingBuffer} by the producer. */
    public long chunksIn = 0L;

    /**
     * The #of elements in the chunks written on the {@link BlockingBuffer} by
     * the producer.
     */
    public long elementsIn = 0L;
    
    /**
     * The #of elements in the output chunks written onto the index
     * partitions (not including any eliminated duplicates).
     */
    public long elementsOut = 0L;
    
//    /**
//     * The #of duplicates which were filtered out.
//     */
//    public long duplicateCount = 0L;
    
    /**
     * The #of chunks written onto index partitions using RMI.
     */
    public long chunksOut = 0L;
    
    /**
     * Elapsed nanoseconds for RMI requests.
     */
    public long elapsedNanos = 0L;

    /**
     * Map for the per-index partition statistics. This ensures that we can
     * report the aggregate statistics in detail.
     */
    private final Map<L, HS> partitions = new LinkedHashMap<L, HS>();

    /**
     * Return the statistics object for the specified index partition and never
     * <code>null</code> (a new instance is created if none exists).
     * 
     * @param locator
     *            The index partition.
     * 
     * @return The statistics for that index partition.
     */
    public HS getSubtaskStats(final L locator) {

        synchronized (partitions) {
         
            HS t = partitions.get(locator);

            if (t == null) {

                t = newSubtaskStats(locator);

                partitions.put(locator, t);

            }

            return t;

        }

    }

    /**
     * Factory for the subtask statistics.
     * 
     * @param locator
     *            The subtask key.
     *            
     * @return The statistics for the subtask.
     */
    protected abstract HS newSubtaskStats(L locator);
    
    /**
     * Return a snapshot of the statistics for each index partition.
     */
    public IndexPartitionWriteStats[] getStats() {

        synchronized (partitions) {

            return partitions.values().toArray(
                    new IndexPartitionWriteStats[partitions.size()]);
            
        }

    }

    public AbstractMasterStats() {

    }
 
    /**
     * Return a {@link CounterSet} which may be used to report the statistics on
     * the index write operation. The {@link CounterSet} is NOT placed into any
     * namespace.
     */
    public CounterSet getCounterSet() {
        
        final CounterSet t = new CounterSet();
        
        t.addCounter("indexPartitionStartCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(subtaskStartCount);
            }
        });

        t.addCounter("indexPartitionDoneCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(subtaskEndCount);
            }
        });

        t.addCounter("staleLocatorCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(staleLocatorCount);
            }
        });

        t.addCounter("chunksIn", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(chunksIn);
            }
        });

        t.addCounter("elementsIn", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elementsIn);
            }
        });

        t.addCounter("chunksOut", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(chunksOut);
            }
        });

        t.addCounter("elementsOut", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elementsOut);
            }
        });

//        t.addCounter("duplicateCount", new Instrument<Long>() {
//            @Override
//            protected void sample() {
//                setValue(duplicateCount);
//            }
//        });

        t.addCounter("elapsedNanos", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elapsedNanos);
            }
        });

        t.addCounter("averageNanos/write", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(getAverageNanosPerWrite());
            }
        });

        t.addCounter("averageElements/write", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue((chunksOut == 0L ? 0 : elementsOut
                        / (double) chunksOut));
            }
        });

        return t;

    }

    /**
     * The average #of nanoseconds per chunk written on the index partition.
     */
    public double getAverageNanosPerWrite() {

        return (chunksOut == 0L ? 0 : elapsedNanos / (double) chunksOut);

    }

    /**
     * The average #of elements (tuples) per chunk written on the index
     * partition.
     */
    public double getAverageElementsPerWrite() {

        return (chunksOut == 0L ? 0 : elementsOut / (double) chunksOut);

    }

    public String toString() {

        return getClass().getName() + "{indexPartitionStartCount="
                + subtaskStartCount + ", indexPartitionDoneCount="
                + subtaskEndCount + ", staleLocatorCount="
                + staleLocatorCount + ", chunkIn=" + chunksIn + ", elementIn="
                + elementsIn 
//                + ", duplicateCount=" + duplicateCount
                + ", chunksOut=" + chunksOut + ", elementsOut=" + elementsOut
                + ", elapsedNanos=" + elapsedNanos + ", averageNanos/write="
                + getAverageNanosPerWrite() + ", averageElements/write="
                + getAverageElementsPerWrite() + "}";

    }

}
