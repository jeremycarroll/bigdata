/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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
package com.bigdata.journal.jini.ha;

import java.lang.ref.WeakReference;
import java.util.Calendar;
import java.util.Formatter;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.HASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;

/**
 * Policy schedules a snapshot at the same time each day. A threshold is used to
 * skip the backup if the HALog delta is LTE a specified percentage of the size
 * of the journal on the disk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DefaultSnapshotPolicy implements ISnapshotPolicy {

    private static final transient Logger log = Logger
            .getLogger(DefaultSnapshotPolicy.class);
    
    final private int timeOfDay;
    final private int percentLogSize;

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder();

        final Formatter f = new Formatter(sb);

        f.format("%04d", timeOfDay);
        f.flush();
        f.close();
        
        final String todStr = sb.toString();

        return DefaultSnapshotPolicy.class.getSimpleName() + "{timeOfDay="
                + todStr + ", percentLogSize=" + percentLogSize + "%}";

    }
    
    /**
     * The default policy wakes up at <code>0200</code> and takes a snapshot if
     * the size of the HALogs written since the last snapshot is at least
     * <code>50%</code> of the size of the journal on the disk.
     */
    public DefaultSnapshotPolicy() {

        this(200/* 0200 hours */, 50/* percent */);

    }

    /**
     * 
     * @param timeOfDay
     *            The time of day to wake up and decide whether or not to make a
     *            new snapshot.
     * @param percentLogSize
     *            The threshold at which a new snapshot will be made. This is
     *            expressed as a percentage of the HALog size on the disk for
     *            those HALog files written since the last snapshot (or all
     *            HALogs if no snapshot has been taken yet).
     */
    public DefaultSnapshotPolicy(final int timeOfDay, final int percentLogSize) {

        if (timeOfDay < 0)
            throw new IllegalArgumentException();

        if (percentLogSize < 0 || percentLogSize > 400)
            throw new IllegalArgumentException("percentage must be in [0:400]");

        this.timeOfDay = timeOfDay;

        this.percentLogSize = percentLogSize;
        
    }

    public void init(final HAJournal jnl) {
        
        final long initialDelay = delay(timeOfDay);

        if (log.isInfoEnabled())
            log.info("initialDelay=" + initialDelay + "ms" + " (hours="
                    + TimeUnit.MILLISECONDS.toHours(initialDelay)
                    + ", minutes="
                    + (TimeUnit.MILLISECONDS.toMinutes(initialDelay) % 60L)
                    + ")");

        jnl.addScheduledTask(new SnapshotTask(jnl), initialDelay, 1/* delay */,
                TimeUnit.DAYS);

    }

    private class SnapshotTask implements Runnable {
        
        /**
         * Note: Weak reference prevents the Journal from being pinned.
         */
        private final WeakReference<HAJournal> ref;
        
        public SnapshotTask(final HAJournal jnl) {

            this.ref = new WeakReference<HAJournal>(jnl);

        }

        /**
         * Note: Do not throw anything out of this method or it will cause the
         * task to not be rescheduled!
         */
        @Override
        public void run() {

            try {

                final HAJournal jnl = ref.get();

                if (jnl == null) {

                    // Journal reference has been cleared.
                    return;
                    
                }

                // Conditionally start a snapshot.
                final Future<IHASnapshotResponse> f = jnl.getSnapshotManager()
                        .takeSnapshot(new HASnapshotRequest(percentLogSize));

                if (f != null) {

                    // Wait for the snapshot to complete.
                    f.get();

                }

            } catch (Throwable t) {
                
                log.error(t, t);
                
            }

        }
        
    }
    
    /**
     * Return the delay (milliseconds) until the given time of day. The time of
     * day is expressed as a single integer <code>hhmm</code>.
     * 
     * @param tod
     *            The time of day expressed as a single integer
     *            <code>hhmm</code>.
     * 
     * @return The milliseconds until that time of day.
     */
    static private long delay(final int tod) {
       
        final long minuteMillis = 60 * 1000;
        final long dayMillis = 24 * 60 * minuteMillis;

        final int todHours = tod / 100;
        final int todMinutes = tod % 100;

        final long todMillis = ((todHours * 60) + todMinutes) * minuteMillis;

        final long now = System.currentTimeMillis();

        final long tzAdjust = Calendar.getInstance().getTimeZone()
                .getRawOffset();

        // days mod 24 * 60 * 60 * 1000
        final long startOfDay = now - (now % dayMillis) - tzAdjust;
        
//        final long startOfDay = now - (now % dayMillis);

        final long targetTime = startOfDay + todMillis;

        final long delayMs = targetTime - now;

        if (delayMs < 0) {
        
            return delayMs + dayMillis;
            
        } else {
            
            return delayMs;
            
        }

    }

//    public static void main(String[] args) {
//        System.out.println("1540 delay: " + delay(1540));
//        System.out.println("1330 delay: " + delay(1330));
//    }
    
}
