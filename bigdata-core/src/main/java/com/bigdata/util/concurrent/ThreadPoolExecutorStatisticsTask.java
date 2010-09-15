package com.bigdata.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.util.concurrent.IQueueCounters.ITaskCounters;
import com.bigdata.util.concurrent.IQueueCounters.IThreadPoolExecutorCounters;
import com.bigdata.util.concurrent.IQueueCounters.IThreadPoolExecutorTaskCounters;
import com.bigdata.util.concurrent.IQueueCounters.IWriteServiceExecutorCounters;

/**
 * Class tracks a variety of information about a {@link ThreadPoolExecutor}
 * including the moving average of its queue length, queuing times, etc.
 * 
 * @todo refactor to layer {@link QueueSizeMovingAverageTask} then
 *       {@link ThreadPoolExecutorBaseStatisticsTask}, then this class, then sub-classes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThreadPoolExecutorStatisticsTask<EXEC extends ThreadPoolExecutor,COUNTERS extends TaskCounters> implements Runnable {

    protected static final Logger log = Logger.getLogger(ThreadPoolExecutorStatisticsTask.class);
    
    /**
     * The label for the executor service (used in log messages).
     */
    protected final String serviceName;
    
    /**
     * The executor service that is being monitored.
     */
    protected final EXEC service;

//    /**
//     * The time when we started to collect data about the {@link #service} (set by the ctor).
//     */
//    private final long startNanos;
    
    /**
     * The weight used to compute the moving average.
     */
    protected final double w;

    /**
     * #of samples taken so far.
     */
    protected long nsamples = 0;
    
    /*
     * There are several different moving averages which are computed.
     */
    
//    private double averageQueueSize   = 0d;
    protected double averageActiveCount = 0d;
    protected double averageQueueLength = 0d;
    
    /**
     * Data collected about {@link AbstractTask}s run on a service (optional).
     */
    protected final COUNTERS taskCounters;
    
    /*
     * These are moving averages based on the optional TaskCounters.
     * 
     * Note: We also store the old value for these since they are cumulative
     * counters but we need only the delta from the last value for the counter
     * to compute the moving averages.
     * 
     * @todo there are other data that could be computed and reported out.
     */

    /** time waiting on the queue until the task begins to execute. */
    protected double averageQueueWaitingTime = 0d;

    /** time doing work (does not include time to acquire resources locks or commit time). */
    protected double averageServiceTime = 0d;
    /** time checkpointing indices (included in the {@link #averageServiceTime}). */
    protected double averageCheckpointTime = 0d;

    /** total time from submit to completion. */
    protected double averageQueuingTime = 0d;

    /*
     * private variables used to compute the delta in various counters since
     * they were last sampled.
     */
    protected long queueWaitingTime = 0L;
    protected long serviceTime = 0L;
    protected long checkpointTime = 0L; // Note: checkpointTime is included in the serviceTime.
    protected long queuingTime = 0L;

    /**
     * Scaling factor converts nanoseconds to milliseconds.
     */
    static final protected double scalingFactor = 1d / TimeUnit.NANOSECONDS.convert(1,
            TimeUnit.MILLISECONDS);
    
    /**
     * The weight used to compute the moving average.
     */
    public double getWeight() {
        
        return w;
        
    }
    
    /**
     * #of samples taken so far.
     */
    public long getSampleCount() {
        
        return nsamples;
        
    }
    
    /**
     * The recommended default weight.
     */
    public static final double DEFAULT_WEIGHT = .2d;
    
    /**
     * Ctor variant when the {@link ThreadPoolExecutor} does not have hooks for
     * an {@link AbstractTask} and therefore does not update
     * {@link TaskCounters}s.
     * 
     * @param serviceName
     *            The label for the service.
     * 
     * @param service
     *            The service to be monitored.
     */
    public ThreadPoolExecutorStatisticsTask(String serviceName, EXEC service) {

        this(serviceName, service, null/* taskCounters */, DEFAULT_WEIGHT);

    }

    /**
     * Ctor variant when the {@link ThreadPoolExecutor} has hooks for an
     * {@link AbstractTask} and updates the given {@link TaskCounters}s.
     * 
     * @param serviceName
     *            The label for the service.
     * @param service
     *            The service to be monitored.
     * @param taskCounters
     *            The per-task counters used to compute the latency data for
     *            tasks run on that service.
     */
    public ThreadPoolExecutorStatisticsTask(String serviceName, EXEC service,
            COUNTERS taskCounters) {

        this(serviceName, service, taskCounters, DEFAULT_WEIGHT);

    }
    
    /**
     * Core impl.
     * 
     * @param serviceName
     *            The label for the service.
     * @param service
     *            The service to be monitored.
     * @param taskCounters
     *            The per-task counters used to compute the latency data for
     *            tasks run on that service.
     * @param w
     *            The weight to be used by
     *            {@link #getMovingAverage(double, double, double)}
     */
    public ThreadPoolExecutorStatisticsTask(String serviceName, EXEC service,
            COUNTERS taskCounters, double w) {
    
        if (serviceName == null)
            throw new IllegalArgumentException();

        if (service == null)
            throw new IllegalArgumentException();

        // Note: MAY be null (useful for generic executor services).
//        if (taskCounters == null)
//            throw new IllegalArgumentException();

        if (w <= 0d || w >= 1d)
            throw new IllegalArgumentException();
        
        this.serviceName = serviceName;
        
        this.service = service;
        
//        this.startNanos = System.nanoTime();
        
        this.taskCounters = taskCounters;
        
        this.w = w;
        
    }
    
    /**
     * Compute a moving average: <code>(1 - w) * avg + w * q</code>
     * 
     * @param avg
     *            The previous average and initially zero (0.0).
     * @param q
     *            The current value (e.g., the instantaneous measurement of the
     *            #of active tasks plus the length of the queue).
     * @param w
     *            The weight for the moving average in (0:1). Values around .2
     *            seem appropriate.
     * 
     * @return The updated moving average.
     */
    public double getMovingAverage(double avg, double q, double w) {
        
        return (1 - w) * avg + (w * q);
        
    }

    /**
     * The moving average of the queue size.
     */
    protected final MovingAverageTask queueSizeTask = new MovingAverageTask(
            "queueSize", new Callable<Integer>() {
                public Integer call() {
                    return service.getQueue().size();
                }
            });

    /**
     * The moving average of the change in the total inter-arrival time.
     * 
     * @see TaskCounters#interArrivalNanoTime
     */
    protected DeltaMovingAverageTask interArrivalNanoTimeTask = new DeltaMovingAverageTask(
            "interArrivalTime", new Callable<Long>() {
                public Long call() {
                    return taskCounters.interArrivalNanoTime.get();
                }
            });

    /**
     * The moving average of the change in the total task service time.
     * 
     * @see TaskCounters#serviceNanoTime
     */
    protected DeltaMovingAverageTask serviceNanoTimeTask = new DeltaMovingAverageTask(
            "serviceNanoTime", new Callable<Long>() {
                public Long call() {
                    return taskCounters.serviceNanoTime.get();
                }
            });
    
    /**
     * This should be invoked once per second to sample various counters in
     * order to turn their values into moving averages.
     * <p>
     * Note: don't throw anything from here or it will cause the scheduled task
     * executing this to no longer be run!
     */
    public void run() {

        try {

            calculateAll();
            
            nsamples++;

        } catch (Exception ex) {

            log.warn(serviceName, ex);

        }
        
    }

    /**
     *
     * Calculates all averages and updates the task counters if provided.
     * Currently this method calculates the basic executor queue info, and if task counters are present,
     * will also calculate queue wait time and checkpoint times.  All of these calculations are done in seperate methods
     * so that subclasses can override them if needed.
     *
     */
    protected void calculateAll() {
        calculateBasicQueueInfo();
        calculateTaskCountersIfPresent();
    }

    /**
     * Calculates average queue wait and checkpoint times if a task counter was provided.  Sub-classes may override this
     * method to introduce additional task counter calculations.
     */
    protected void calculateTaskCountersIfPresent() {
        if (taskCounters != null) {

            /*
             * Compute some latency data that relies on the task counters.
             */

            // #of tasks that have been submitted so far.
            final long taskCount = taskCounters.taskCompleteCount.get();

            if (taskCount > 0) {

                calculateAverageQueueWait();

                calculateAverageCheckpointTime();

            }

        }
    }

    protected void calculateAverageCheckpointTime() {
        /*
    * Time that the task is being serviced (after its obtained
    * any locks).
    */
        {

            final long newValue = taskCounters.serviceNanoTime.get();

            final long delta = newValue - serviceTime;

            assert delta >= 0 : "" + delta;

            serviceTime = newValue;

            averageServiceTime = getMovingAverage(
                    averageServiceTime,
                    (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                    w);

        }

        /*
    * The moving average of the change in the cumulative
    * inter-arrival time.
    */
        interArrivalNanoTimeTask.run();

        /*
    * The moving average of the change in the total task
    * service time.
    */
        serviceNanoTimeTask.run();

        /*
    * Time that the task is busy checkpoint its indices (this
    * is already reported as part of the service time but which
    * is broken out here as a detail).
    */
        {

            final long newValue = taskCounters.checkpointNanoTime.get();

            final long delta = newValue - checkpointTime;

            assert delta >= 0 : "" + delta;

            checkpointTime = newValue;

            averageCheckpointTime = getMovingAverage(
                    averageCheckpointTime,
                    (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                    w);

        }

        /*
    * Queuing time (elapsed time from submit until completion).
    */
        {

            final long newValue = taskCounters.queuingNanoTime.get();

            final long delta = newValue - queuingTime;

            assert delta >= 0 : "" + delta;

            queuingTime = newValue;

            averageQueuingTime = getMovingAverage(
                    averageQueuingTime,
                    (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                    w);

        }
    }



    protected void calculateAverageQueueWait() {
        /*
    * Time waiting on the queue to begin execution.
    */
        {

            final long newValue = taskCounters.queueWaitingNanoTime.get();

            final long delta = newValue - queueWaitingTime;

            assert delta >= 0 : "" + delta;

            queueWaitingTime = newValue;

            averageQueueWaitingTime = getMovingAverage(
                    averageQueueWaitingTime,
                    (delta * scalingFactor / taskCounters.taskCompleteCount.get()),
                    w);

        }
    }

    protected void calculateBasicQueueInfo() {
        queueSizeTask.run();

        // queueSize := #of tasks in the queue.
        final int queueSize = service.getQueue().size();

        // activeCount := #of tasks assigned a worker thread
        final int activeCount = service.getActiveCount();

////                 This is just the tasks that are currently waiting to run (not
////                 assigned to any thread).
//                averageQueueSize = getMovingAverage(averageQueueSize,
//                        queueSize, w);

        // This is just the tasks that are currently running (assigned
        // to a worker thread).
        averageActiveCount = getMovingAverage(averageActiveCount,
                activeCount, w);

        /*
    * Note: this is the primary average of interest - it includes
    * both the tasks waiting to be run and those that are currently
    * running in the definition of the "queue length".
    */
        averageQueueLength = getMovingAverage(averageQueueLength,
                (activeCount + queueSize), w);
    }

    /**
     * Convenience call to generate a counter set.  Currently creates a new CounterSet and calls fillCounterSet
     * to populate the data.
     * 
     * @return A newly created and filled <i>counterSet</i>
     */
    public CounterSet getCounters() {

        final CounterSet counterSet = new CounterSet();
        fillCounterSet(counterSet);


        return counterSet;

}

    /**
     * Adds counters for all innate variables defined for a
     * {@link ThreadPoolExecutor} and for each of the variables computed by this
     * class. Sub-classes can override this method to fill in additional counters in the provided counter set.
     *
     * @param counterSet the set that will have the counters added to it.
     *
     */
    protected void fillCounterSet(CounterSet counterSet) {
        /*
        * Defined for ThreadPoolExecutor.
        */

//        Note: reported as moving average instead.
//        counterSet.addCounter("#active",
//                new Instrument<Integer>() {
//                    public void sample() {
//                        setValue(service.getActiveCount());
//                    }
//                });
//
//      Note: reported as moving average instead.
//        counterSet.addCounter("#queued",
//                new Instrument<Integer>() {
//                    public void sample() {
//                        setValue(service.getQueue().size());
//                    }
//                });
//

        /*
         * Computed variables based on just the Queue.
         */
        {

//            counterSet.addCounter(IQueueCounters.AverageQueueSize,
//                    new Instrument<Double>() {
//                        @Override
//                        protected void sample() {
//                            setValue(averageQueueSize);
//                        }
//                    });

            counterSet.addCounter(
                    IQueueCounters.AverageQueueSize,
                    new Instrument<Double>() {
                        @Override
                        protected void sample() {
                            setValue(queueSizeTask
                                    .getMovingAverage());
                        }
                    });

        }

        /*
         * Computed variables based on the service itself.
         */
        {

            if (taskCounters == null) {

                /*
                 * Report iff not being reported via the TaskCounters.
                 */

                counterSet.addCounter(
                        IThreadPoolExecutorCounters.TaskCompleteCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(service.getCompletedTaskCount());
                            }
                        });

            }

            counterSet.addCounter(IThreadPoolExecutorCounters.PoolSize,
                    new Instrument<Integer>() {
                        public void sample() {
                            setValue(service.getPoolSize());
                        }
                    });

            counterSet.addCounter(IThreadPoolExecutorCounters.LargestPoolSize,
                    new Instrument<Integer>() {
                        public void sample() {
                            setValue(service.getLargestPoolSize());
                        }
                    });

            counterSet.addCounter(
                    IThreadPoolExecutorCounters.AverageActiveCount,
                    new Instrument<Double>() {
                        @Override
                        protected void sample() {
                            setValue(averageActiveCount);
                        }
                    });

            counterSet.addCounter(
                    IThreadPoolExecutorCounters.AverageQueueLength,
                    new Instrument<Double>() {
                        @Override
                        protected void sample() {
                            setValue(averageQueueLength);
                        }
                    });

        }

        /*
         * Computed variables based on the per-task counters.
         */
        if (taskCounters != null) {

            /*
             * Simple counters.
             */

            // count of all tasks submitted to the service.
            counterSet.addCounter(
                    IThreadPoolExecutorTaskCounters.TaskSubmitCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(taskCounters.taskSubmitCount.get());
                        }
                    });

            // count of all tasks completed by the service (failed +
            // success).
            counterSet.addCounter(
                    IThreadPoolExecutorCounters.TaskCompleteCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(taskCounters.taskCompleteCount.get());
                        }
                    });

            // count of all tasks which failed during execution.
            counterSet.addCounter(
                    IThreadPoolExecutorTaskCounters.TaskFailCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(taskCounters.taskFailCount.get());
                        }
                    });

            // count of all tasks which were successfully executed.
            counterSet.addCounter(
                    IThreadPoolExecutorTaskCounters.TaskSuccessCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(taskCounters.taskSuccessCount.get());
                        }
                    });

            /*
             * Running totals for various things.
             */

            {

                counterSet.addCounter(ITaskCounters.InterArrivalTime,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(TimeUnit.NANOSECONDS
                                        .toMillis(taskCounters.interArrivalNanoTime
                                                .get()));
                            }
                        });

                counterSet.addCounter(ITaskCounters.QueueWaitingTime,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(TimeUnit.NANOSECONDS
                                        .toMillis(taskCounters.queueWaitingNanoTime
                                                .get()));
                            }
                        });

                counterSet.addCounter(ITaskCounters.ServiceTime,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(TimeUnit.NANOSECONDS
                                        .toMillis(taskCounters.serviceNanoTime
                                                .get()));
                            }
                        });

                counterSet.addCounter(ITaskCounters.CheckpointTime,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(TimeUnit.NANOSECONDS
                                        .toMillis(taskCounters.checkpointNanoTime
                                                .get()));
                            }
                        });

                counterSet.addCounter(ITaskCounters.QueuingTime,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(TimeUnit.NANOSECONDS
                                        .toMillis(taskCounters.queuingNanoTime
                                                .get()));
                            }
                        });

            }

            /*
             * Moving averages.
             */

            counterSet.addCounter(
                    IWriteServiceExecutorCounters.AverageArrivalRate,
                    new Instrument<Double>() {
                        @Override
                        protected void sample() {
                            final double t = interArrivalNanoTimeTask
                                    .getMovingAverage()
                                    * scalingFactor;
                            if (t != 0d)
                                setValue(1d / t);
                        }
                    });

            counterSet.addCounter(
                    IWriteServiceExecutorCounters.AverageServiceRate,
                    new Instrument<Double>() {
                        @Override
                        protected void sample() {
                            final double t = serviceNanoTimeTask
                                    .getMovingAverage()
                                    * scalingFactor;
                            if (t != 0d)
                                setValue(1d / t);
                        }
                    });

            counterSet
                    .addCounter(
                            IThreadPoolExecutorTaskCounters.AverageQueueWaitingTime,
                            new Instrument<Double>() {
                                @Override
                                protected void sample() {
                                    setValue(averageQueueWaitingTime);
                                }
                            });

            counterSet.addCounter(
                    IThreadPoolExecutorTaskCounters.AverageServiceTime,
                    new Instrument<Double>() {
                        @Override
                        protected void sample() {
                            setValue(averageServiceTime);
                        }
                    });

            counterSet.addCounter(
                    IThreadPoolExecutorTaskCounters.AverageQueuingTime,
                    new Instrument<Double>() {
                        @Override
                        protected void sample() {
                            setValue(averageQueuingTime);
                        }
                    });

        }
    }


}
