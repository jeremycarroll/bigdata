/*
 * User: gossard
 * Date: Sep 15, 2010
 * Time: 1:44:48 PM
 */
package com.bigdata.journal;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.util.concurrent.IQueueCounters;
import com.bigdata.util.concurrent.ThreadPoolExecutorStatisticsTask;
import com.bigdata.util.concurrent.WriteTaskCounters;

/**
 * Extension of {@link com.bigdata.util.concurrent.ThreadPoolExecutorStatisticsTask} that will collect additional
 * information about {@link WriteExecutorService} pools.
 *
 */
public class WriteExecutorServiceStatisticsTask extends ThreadPoolExecutorStatisticsTask<WriteExecutorService,WriteTaskCounters> {
  //TODO: The chaining on the calculateXXX methods is probably too fragile.  In order to guarantee the exact same behavior,
  //TODO: I chained the calls off methods that executed directly before them.  If there is no ordering dependency, the calculations
  //TODO: could be done after the parents calculateAll().  -gossard

    protected double averageActiveCountWithLocksHeld = 0d;
    /** time waiting for resource locks. */
    protected double averageLockWaitingTime = 0d;
    protected double averageCommitWaitingTime = 0d;
    protected double averageCommitServiceTime = 0d;
    protected double averageCommitGroupSize = 0d;
    protected double averageByteCountPerCommit = 0d;
    protected long lockWaitingTime = 0L;
    protected long commitWaitingTime = 0L;
    protected long commitServiceTime = 0L;
    protected double averageReadyCount;


    public WriteExecutorServiceStatisticsTask(String serviceName, WriteExecutorService service) {
        super(serviceName, service);
    }

    public WriteExecutorServiceStatisticsTask(String serviceName, WriteExecutorService service, WriteTaskCounters taskCounters) {
        super(serviceName, service, taskCounters);
    }

    public WriteExecutorServiceStatisticsTask(String serviceName, WriteExecutorService service, WriteTaskCounters taskCounters, double w) {
        super(serviceName, service, taskCounters, w);
    }


    @Override
    protected void calculateBasicQueueInfo() {
        super.calculateBasicQueueInfo();
        calculateActiveCountWithLocks();
    }

    @Override
    protected void calculateAverageQueueWait() {
        super.calculateAverageQueueWait();
        calculateAverageLockWaitTime();
    }

    @Override
    protected void calculateAverageCheckpointTime() {
        super.calculateAverageCheckpointTime();
        calculateWriteSpecificValues();
    }

    protected void calculateWriteSpecificValues() {

        final WriteExecutorService tmp = service;

        final WriteTaskCounters finalWriteTaskCounters = taskCounters;

        final long groupCommitCount = tmp.getGroupCommitCount();

        if (groupCommitCount > 0) {

            // Time waiting for the commit.
            {

                final long newValue = finalWriteTaskCounters.commitWaitingNanoTime
                        .get();

                final long delta = newValue - commitWaitingTime;

                assert delta >= 0 : "" + delta;

                commitWaitingTime = newValue;

                averageCommitWaitingTime = getMovingAverage(
                        averageCommitWaitingTime,
                        (delta * scalingFactor / groupCommitCount),
                        w);

            }

            // Time servicing the commit.
            {

                final long newValue = finalWriteTaskCounters.commitServiceNanoTime
                        .get();

                final long delta = newValue - commitServiceTime;

                assert delta >= 0 : "" + delta;

                commitServiceTime = newValue;

                averageCommitServiceTime = getMovingAverage(
                        averageCommitServiceTime,
                        (delta * scalingFactor / groupCommitCount),
                        w);

            }

        }

        // moving average of the size nready.
        averageReadyCount = getMovingAverage(
                averageReadyCount, tmp.getReadyCount(), w);

        // moving average of the size of the commit groups.
        averageCommitGroupSize = getMovingAverage(
                averageCommitGroupSize, tmp.getCommitGroupSize(), w);

        // moving average of the #of bytes written since the
        // previous commit.
        averageByteCountPerCommit = getMovingAverage(
                averageByteCountPerCommit, tmp
                        .getByteCountPerCommit(), w);

    }

    protected void calculateAverageLockWaitTime() {
        /*
        * Time waiting on resource lock(s).
        */

        final long newValue = taskCounters.lockWaitingNanoTime.get();

        final long delta = newValue - lockWaitingTime;

        assert delta >= 0 : "" + delta;

        lockWaitingTime = newValue;

        averageLockWaitingTime = getMovingAverage(
                averageLockWaitingTime,
                (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                w);

    }

    protected void calculateActiveCountWithLocks() {
        /*
         * Note: For the WriteExecutorService we compute a variant of
         * [activeCount] the which only counts tasks that are currently
         * holding their exclusive resource lock(s). This is the real
         * concurrency of the write service since tasks without locks
         * are waiting on other tasks so that they can obtain their
         * lock(s) and "run".
         */

        final int activeCountWithLocksHeld = service.getActiveTaskCountWithLocksHeld();

        averageActiveCountWithLocksHeld = getMovingAverage(
                averageActiveCountWithLocksHeld, activeCountWithLocksHeld, w);

    }




    @Override
    protected void fillCounterSet(CounterSet counterSet) {
        super.fillCounterSet(counterSet);//fills out all the basic executor queue info.

        final WriteExecutorService writeService = service;

        /*
         * Simple counters.
         */

        counterSet.addCounter(IQueueCounters.IWriteServiceExecutorCounters.CommitCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(writeService.getGroupCommitCount());
                    }
                });

        counterSet.addCounter(IQueueCounters.IWriteServiceExecutorCounters.AbortCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(writeService.getAbortCount());
                    }
                });

        counterSet.addCounter(IQueueCounters.IWriteServiceExecutorCounters.OverflowCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(writeService.getOverflowCount());
                    }
                });

        counterSet.addCounter(IQueueCounters.IWriteServiceExecutorCounters.RejectedExecutionCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(writeService
                                .getRejectedExecutionCount());
                    }
                });

        /*
         * Maximum observed values.
         */

        counterSet.addCounter(IQueueCounters.IWriteServiceExecutorCounters.MaxCommitWaitingTime,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(writeService.getMaxCommitWaitingTime());
                    }
                });

        counterSet.addCounter(IQueueCounters.IWriteServiceExecutorCounters.MaxCommitServiceTime,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(writeService.getMaxCommitServiceTime());
                    }
                });

        counterSet.addCounter(IQueueCounters.IWriteServiceExecutorCounters.MaxCommitGroupSize,
                new Instrument<Long>() {
                    public void sample() {
                        setValue((long) writeService
                                .getMaxCommitGroupSize());
                    }
                });

        counterSet.addCounter(IQueueCounters.IWriteServiceExecutorCounters.MaxRunning,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(writeService.getMaxRunning());
                    }
                });

        /*
         * Moving averages available only for the write executor
         * service.
         */

        counterSet
                .addCounter(
                        IQueueCounters.IWriteServiceExecutorCounters.AverageActiveCountWithLocksHeld,
                        new Instrument<Double>() {
                            @Override
                            protected void sample() {
                                setValue(averageActiveCountWithLocksHeld);
                            }
                        });

        counterSet.addCounter(
                IQueueCounters.IWriteServiceExecutorCounters.AverageReadyCount,
                new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        setValue(averageReadyCount);
                    }
                });

        counterSet.addCounter(
                IQueueCounters.IWriteServiceExecutorCounters.AverageCommitGroupSize,
                new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        setValue(averageCommitGroupSize);
                    }
                });

        counterSet.addCounter(
                IQueueCounters.IWriteServiceExecutorCounters.AverageLockWaitingTime,
                new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        setValue(averageLockWaitingTime);
                    }
                });

        counterSet.addCounter(
                IQueueCounters.IWriteServiceExecutorCounters.AverageCheckpointTime,
                new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        setValue(averageCheckpointTime);
                    }
                });

        counterSet.addCounter(
                IQueueCounters.IWriteServiceExecutorCounters.AverageCommitWaitingTime,
                new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        setValue(averageCommitWaitingTime);
                    }
                });

        counterSet.addCounter(
                IQueueCounters.IWriteServiceExecutorCounters.AverageCommitServiceTime,
                new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        setValue(averageCommitServiceTime);
                    }
                });

        counterSet
                .addCounter(
                        IQueueCounters.IWriteServiceExecutorCounters.AverageByteCountPerCommit,
                        new Instrument<Double>() {
                            @Override
                            protected void sample() {
                                setValue(averageByteCountPerCommit);
                            }
                        });

    }
}
