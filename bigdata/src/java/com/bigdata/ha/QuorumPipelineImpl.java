/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
package com.bigdata.ha;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.HAMessageWrapper;
import com.bigdata.ha.msg.HASendState;
import com.bigdata.ha.msg.IHAMessage;
import com.bigdata.ha.msg.IHASendState;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.pipeline.HAReceiveService;
import com.bigdata.ha.pipeline.HAReceiveService.IHAReceiveCallback;
import com.bigdata.ha.pipeline.HASendService;
import com.bigdata.ha.pipeline.ImmediateDownstreamReplicationException;
import com.bigdata.ha.pipeline.NestedPipelineException;
import com.bigdata.ha.pipeline.PipelineImmediateDownstreamReplicationException;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.quorum.QCE;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumMember;
import com.bigdata.quorum.QuorumStateChangeEvent;
import com.bigdata.quorum.QuorumStateChangeEventEnum;
import com.bigdata.quorum.QuorumStateChangeListener;
import com.bigdata.quorum.QuorumStateChangeListenerBase;
import com.bigdata.util.InnerCause;

/**
 * {@link QuorumPipeline} implementation.
 * <p>
 * The {@link QuorumMember} must pass along the "pipeline" messages, including:
 * <ul>
 * <li>{@link QuorumMember#pipelineAdd()}</li>
 * <li>{@link QuorumMember#pipelineRemove()}</li>
 * <li>{@link QuorumMember#pipelineChange(UUID, UUID)}</li>
 * </ul>
 * When a quorum is met, the <i>leader</i> is always first in the write pipeline
 * since it is the node which receives writes from clients. When a service joins
 * the write pipeline, it always does so at the end of the chain. Services may
 * enter the write pipeline before joining a quorum in order to synchronize with
 * the quorum. If a service in the middle of the chain leaves the pipeline, then
 * the upstream node will reconfigure and retransmit the current cache block to
 * its new downstream node. This prevent nodes which are "bouncing" during
 * synchronization from causing write sets to be discarded. However, if the
 * leader leaves the write pipeline, then the quorum is broken and the write set
 * will be discarded.
 * <p>
 * Since the write pipeline is used to synchronize services trying to join the
 * quorum as well as the replicate writes for services joined with the quorum,
 * {@link HAReceiveService} may be live for a met quorum even though the
 * {@link QuorumMember} on whose behalf this class is acting is not joined with
 * the met quorum.
 * 
 * <h3>Pipeline maintenance</h3>
 * 
 * There are three broad categories which have to be handled: (1) leader leaves;
 * (2) pipeline leader election; and (3) follower leaves. A leader leave causes
 * the quorum to break, which will cause service leaves and pipeline leaves for
 * all joined services. However, services must add themselves to the pipeline
 * before they join the quorum and the pipeline will be reorganized if necessary
 * when the quorum leader is elected. This will result in a
 * {@link #pipelineElectedLeader()} event. A follower leave only causes the
 * follower to leave the pipeline and results in a
 * {@link #pipelineChange(UUID, UUID)} event.
 * <p>
 * There are two cases for a follower leave: (A) when the follower did not did
 * not have a downstream node; and (B) when there is downstream node. For (B),
 * the upstream node from the left follower should reconfigure for the new
 * downstream node and retransmit the current cache block and the event should
 * be otherwise unnoticed.
 * <p>
 * Handling a follower join requires us to synchronize the follower first which
 * requires some more infrastructure and should be done as part of the HA
 * synchronization test suite.
 * <p>
 * What follows is an example of how events will arrive for a quorum of three
 * services: A, B, and C.
 * 
 * <pre>
 * A.getActor().pipelineAdd() => A.pipelineAdd()
 * B.getActor().pipelineAdd() => B.pipelineAdd(); A.pipelineChange(null,B); 
 * C.getActor().pipelineAdd() => C.pipelineAdd(); B.pipelineChange(null,C);
 * </pre>
 * 
 * At this point the pipeline order is <code>[A,B,C]</code>. Notice that the
 * {@link HASendService} for A is not established until the
 * <code>A.pipelineChange(null,B)</code> sets B as the new downstream service
 * for A. Likewise, B will not relay to C until it handles the
 * <code>B.pipelineChange(null,C)</code> event.
 * 
 * <p>
 * 
 * Given the pipeline order <code>[A,B,C]</code>, if B were to leave, then the
 * events would be:
 * 
 * <pre>
 * B.getActor().pipelineRemove() => B.pipelineRemove(); A.pipelineChange(B,C);
 * </pre>
 * 
 * and when this class handles the <code>A.pipelineChange(B,C)</code> event, it
 * must update the {@link HAReceiveService} such that it now relays data to C.
 * 
 * <p>
 * 
 * On the other hand, given the pipeline order <code>[A,B,C]</code>, if C were
 * to leave the events would be:
 * 
 * <pre>
 * C.getActor().pipelineRemove() => C.pipelineRemove(); B.pipelineChange(C,null);
 * </pre>
 * 
 * and when this class handles the <code>B.pipelineChange(C,null)</code> event,
 * it must update the C's {@link HAReceiveService} such that it continues to
 * receive data, but no longer relays data to a downstream service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <S>
 * 
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/681" > 
 * HAJournalServer deadlock: pipelineRemove() and getLeaderId() </a>
 */
abstract public class QuorumPipelineImpl<S extends HAPipelineGlue> /*extends
        QuorumStateChangeListenerBase */implements QuorumPipeline<S>,
        QuorumStateChangeListener {

    static private transient final Logger log = Logger
            .getLogger(QuorumPipelineImpl.class);

    /**
     * The timeout for a sleep before the next retry. These timeouts are
     * designed to allow some asynchronous processes to reconnect the
     * {@link HASendService} and the {@link HAReceiveService}s in write pipeline
     * such that a retransmit can succeed after a service has left the pipeline.
     * Depending on the nature of the error (i.e., a transient network problem
     * versus a pipeline reorganization), this can involve a number of zookeeper
     * events.
     * <p>
     * The retries will continue until {@link #getRetrySendTimeoutNanos()} has
     * elapsed.
     * 
     * FIXME If this timeout is LTE 50ms, then ChecksumErrors can appear in the
     * sudden kill tests. The root cause is the failure to consistently tear
     * down and setup the pipeline when there are multiple requests queued for
     * the pipeline. This issue has also shown up when there is a replicated
     * write and a concurrent HALog replication. Specifically, an interrupt in
     * sendHALog() does not cause the socket channel on which the payload is
     * transmitted to be closed. See <a
     * href="https://sourceforge.net/apps/trac/bigdata/ticket/724">HA wire
     * pulling and sudden kill testing</a>.
     */
    private final int RETRY_SLEEP = 30; //200; // 50; // milliseconds.
    
    /**
     * Once this timeout is elapsed, retrySend() will fail.
     * <p>
     * Note: This gets overridden in the ZK aware version and set to a constant
     * greater than the then-current negotiated timeout for the client.
     */
    protected long getRetrySendTimeoutNanos() {
    
        return TimeUnit.MILLISECONDS.toNanos(5000/*ms*/);
        
    }

    /**
     * The {@link QuorumMember}.
     */
    private final QuorumMember<S> member;

    /**
     * The service {@link UUID} for the {@link QuorumMember}.
     */
    private final UUID serviceId;
    
    /**
     * Lock managing the various mutable aspects of the pipeline state.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /** send service (iff this is the leader). */
    private HASendService sendService;

    /**
     * The receive service (iff this is a follower in a met quorum).
     */
    private HAReceiveService<HAMessageWrapper> receiveService;

    /**
     * The buffer used to relay the data. This is only allocated for a
     * follower. 
     */
    private IBufferAccess receiveBuffer;
    
    /**
     * Cached metadata about the downstream service.
     */
    private final AtomicReference<PipelineState<S>> pipelineStateRef = new AtomicReference<PipelineState<S>>();

    /**
     * Inner class does the actual work once to handle an event.
     */
    private final InnerEventHandler innerEventHandler = new InnerEventHandler();

    /**
     * One up message identifier.
     */
    private final AtomicLong messageId = new AtomicLong(0L);
    
    /**
     * Core implementation of the handler for the various events. Always run
     * while holding the {@link #lock}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/681" >
     *      HAJournalServer deadlock: pipelineRemove() and getLeaderId() </a>
     */
    private final class InnerEventHandler extends QuorumStateChangeListenerBase {

        /**
         * A queue of events that can only be handled when a write replication
         * operation owns the {@link QuorumPipelineImpl#lock}.
         * 
         * @see QuorumPipelineImpl#lock()
         * @see #dispatchEvents()
         */
        private final BlockingQueue<QuorumStateChangeEvent> queue = new LinkedBlockingQueue<QuorumStateChangeEvent>();

        protected InnerEventHandler() {

        }

        /**
         * Enqueue an event.
         * 
         * @param e
         *            The event.
         */
        private void queue(final QuorumStateChangeEvent e) {

        	if (log.isInfoEnabled())
        		log.info("Adding StateChange: " + e);
        	
            queue.add(e);
            
        }

        /**
         * Boolean controls whether or not event elision is used. See below.
         * 
         * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/681" >
         *      HAJournalServer deadlock: pipelineRemove() and getLeaderId()
         *      </a>
         */        
        static private final boolean s_eventElission = true;
        
        /**
         * Event elission endeavours to ensure that events processed
         * represent current state change.
         * 
         * This is best explained with an example from its original usage
         * in processing graphic events.  Whilst a "button click" is a singular
         * event and all button clicks should be processed, a "mouse move" event
         * could be elided with the next "mouse move" event.  Thus the move events 
         * (L1 -> L2) and (L2 -> L3) would elide to a single (L1 -> L3).
         * 
         * In HA RMI calls can trigger event processing, whilst other threads monitor
         * state changes - such as open sockets.  Without elission, monitoring threads
         * will observe unnecessary transitional state changes.  HOWEVER, there remains
         * a problem with this pattern of synchronization.
         */
        private void elideEvents() {

            if (!s_eventElission) {
                return;
            }
        	
            /*
             * Check for event elission: check for PIPELINE_UPSTREAM and
             * PIPELINE_CHANGE and remove earlier ones check for PIPELINE_ADD
             * and PIPELINE_REMOVE pairings.
             */
            final Iterator<QuorumStateChangeEvent> events = queue.iterator();
            QuorumStateChangeEvent uce = null; // UPSTREAM CHANGE
            QuorumStateChangeEvent dce = null; // DOWNSTREAM CHANGE
            QuorumStateChangeEvent add = null; // PIPELINE_ADD
            		
            while (events.hasNext()) {
                final QuorumStateChangeEvent tst = events.next();
                if (tst.getEventType() == QuorumStateChangeEventEnum.PIPELINE_UPSTREAM_CHANGE) {
                    if (uce != null) {
                        if (log.isDebugEnabled())
                            log.debug("Elission removal of: " + uce);
                        queue.remove(uce);
                    }
                    uce = tst;
                } else if (tst.getEventType() == QuorumStateChangeEventEnum.PIPELINE_CHANGE) {
                    if (dce != null) {
                        // replace 'from' of new state with 'from' of old
                        tst.getDownstreamOldAndNew()[0] = dce
                                .getDownstreamOldAndNew()[0];

                        if (log.isDebugEnabled())
                            log.debug("Elission removal of: " + dce);
                        queue.remove(dce);
                    }
                    dce = tst;
                } else if (tst.getEventType() == QuorumStateChangeEventEnum.PIPELINE_ADD) {
                		add = tst;
                } else if (tst.getEventType() == QuorumStateChangeEventEnum.PIPELINE_REMOVE) {
                    if (add != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Elission removal of: " + add);
                            log.debug("Elission removal of: " + tst);
                        }
                        queue.remove(add);
                        queue.remove(tst);
                        add = null;
                    }
                    if (dce != null) {
                        if (log.isDebugEnabled())
                            log.debug("Elission removal of: " + dce);
                        queue.remove(dce);
                        dce = null;
                    }
                    if (uce != null) {
                        if (log.isDebugEnabled())
                            log.debug("Elission removal of: " + uce);
                        queue.remove(uce);
                        uce = null;
                    }
                }

            }
                    	
        } // elideEvents()
        
        /**
         * Dispatch any events in the {@link #queue}.
         */
        private void dispatchEvents() {

        	elideEvents();

            QuorumStateChangeEvent e;
            
            // If an event is immediately available, dispatch it now.
            while ((e = queue.poll()) != null) {
        
            	if (log.isInfoEnabled())
            		log.info("Dispatching: " + e);

                // An event is available.
                innerEventHandler.dispatchEvent(e);

            }

        }

        /**
         * Dispatch to the InnerEventHandler.
         * 
         * @param e
         *            The event.
         * 
         * @throws IllegalMonitorStateException
         *             if the caller does not own the {@link #lock}.
         */
        private void dispatchEvent(final QuorumStateChangeEvent e)
                throws IllegalMonitorStateException {

            if(!lock.isHeldByCurrentThread()) {
            
                /*
                 * The InnerEventHandler should be holding the outer lock.
                 */
                
                throw new IllegalMonitorStateException();
                
            }
            
            if (log.isInfoEnabled())
                log.info(e.toString());
            
            switch (e.getEventType()) {
            case CONSENSUS:
                consensus(e.getLastCommitTimeConsensus());
                break;
            case LOST_CONSENSUS:
                lostConsensus();
                break;
            case MEMBER_ADD:
                memberAdd();
                break;
            case MEMBER_REMOVE:
                memberRemove();
                break;
            case PIPELINE_ADD:
                pipelineAdd();
                break;
            case PIPELINE_CHANGE: {
                final UUID[] a = e.getDownstreamOldAndNew();
                pipelineChange(a[0]/* oldDownStreamId */, a[1]/* newDownStreamId */);
                break;
            }
            case PIPELINE_ELECTED_LEADER:
                pipelineElectedLeader();
                break;
            case PIPELINE_REMOVE:
                pipelineRemove();
                break;
            case PIPELINE_UPSTREAM_CHANGE:
                pipelineUpstreamChange();
                break;
            case QUORUM_BREAK:
                quorumBreak();
                break;
            case QUORUM_MEET:
                quorumMeet(e.getToken(), e.getLeaderId());
                break;
            case SERVICE_JOIN:
                serviceJoin();
                break;
            case SERVICE_LEAVE:
                serviceLeave();
                break;
            default:
                throw new UnsupportedOperationException(e.getEventType().toString());
            }
        }

//      @Override
//      public void serviceLeave() {
//      }
//      
//      @Override
//      public void serviceJoin() {
//      }
//      
//      /**
//      * Extended to setup this service as a leader ({@link #setUpLeader()}),
//      * or a relay ({@link #setUpReceiveAndRelay()}. 
//      */
//     @Override
//     public void quorumMeet(final long token, final UUID leaderId) {
//         super.quorumMeet(token, leaderId);
//         lock.lock();
//         try {
//             this.token = token;
//             if(leaderId.equals(serviceId)) {
//                 setUpLeader();
//             } else if(member.isPipelineMember()) {
//                 setUpReceiveAndRelay();
//             }
//         } finally {
//             lock.unlock();
//         }        
//     }

//     @Override
//     public void quorumBreak() {
//         super.quorumBreak();
//         lock.lock();
//         try {
//             tearDown();
//         } finally {
//             lock.unlock();
//         }
//     }
      
        /**
         * {@inheritDoc}
         * <p>
         * This implementation sets up the {@link HASendService} or the
         * {@link HAReceiveService} as appropriate depending on whether or not
         * this service is the first in the pipeline order.
         */
        @Override
        public void pipelineAdd() {
            if (log.isInfoEnabled())
                log.info("");
            super.pipelineAdd();
            lock.lock();
            try {
                // The current pipeline order.
                final UUID[] pipelineOrder = member.getQuorum().getPipeline();
                // The index of this service in the pipeline order.
                final int index = getIndex(serviceId, pipelineOrder);
                if (index == 0) {
                    setUpSendService();
                } else if (index > 0) {
                    setUpReceiveService();
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void pipelineElectedLeader() {
            if (log.isInfoEnabled())
                log.info("");
            super.pipelineElectedLeader();
            lock.lock();
            try {
                tearDown();
                setUpSendService();
            } finally {
                lock.unlock();
            }
        }

        /**
         * {@inheritDoc}
         * <p>
         * This implementation tears down the {@link HASendService} or
         * {@link HAReceiveService} associated with this service.
         */
        @Override
        public void pipelineRemove() {
            if (log.isInfoEnabled())
                log.info("");
            super.pipelineRemove();
            lock.lock();
            try {
                tearDown();
            } finally {
                lock.unlock();
            }
        }

        /**
         * {@inheritDoc}
         * <p>
         * This implementation changes the target of the {@link HASendService}
         * for the leader (or the {@link HAReceiveService} for a follower) to
         * send (or relay) write cache blocks to the specified service.
         */
        @Override
        public void pipelineChange(final UUID oldDownStreamId,
                final UUID newDownStreamId) {
            super.pipelineChange(oldDownStreamId, newDownStreamId);
            lock.lock();
            try {
                // The address of the next service in the pipeline.
                final InetSocketAddress addrNext = newDownStreamId == null ? null
                        : getAddrNext(newDownStreamId);
                if (log.isInfoEnabled())
                    log.info("oldDownStreamId=" + oldDownStreamId
                            + ",newDownStreamId=" + newDownStreamId
                            + ", addrNext=" + addrNext + ", sendService="
                            + sendService + ", receiveService="
                            + receiveService);
                if (sendService != null) {
                    /*
                     * Terminate the existing connection (we were the first
                     * service in the pipeline).
                     */
                    sendService.terminate();
                    if (addrNext != null) {
                        if (log.isDebugEnabled())
                            log.debug("sendService.start(): addrNext="
                                    + addrNext);
                        sendService.start(addrNext);
                    }
                } else if (receiveService != null) {
                    /*
                     * Reconfigure the receive service to change how it is
                     * relaying (we were relaying, so the receiveService was
                     * running but not the sendService).
                     */
                    if (log.isDebugEnabled())
                        log.debug("receiveService.changeDownStream(): addrNext="
                                + addrNext);
                    receiveService.changeDownStream(addrNext);
                }
                // populate and/or clear the cache.
                cachePipelineState(newDownStreamId);
                if (log.isDebugEnabled())
                    log.debug("pipelineChange - done.");
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void pipelineUpstreamChange() {
            super.pipelineUpstreamChange();
            lock.lock();
            try {
                if (receiveService != null) {
                    /*
                     * Make sure that the receiveService closes out its client
                     * connection with the old upstream service.
                     */
                    if (log.isInfoEnabled())
                        log.info("receiveService=" + receiveService);
                    receiveService.changeUpStream();
                }
            } finally {
                lock.unlock();
            }
        }
      
//      @Override
//      public void memberRemove() {
//      }
//      
//      @Override
//      public void memberAdd() {
//      }
//      
//      @Override
//      public void lostConsensus() {
//      }
//      
//      @Override
//      public void consensus(long lastCommitTime) {
//      }

        /**
         * Request the {@link InetSocketAddress} of the write pipeline for a service
         * (RMI).
         * 
         * @param downStreamId
         *            The service.
         *            
         * @return It's {@link InetSocketAddress}
         */
        private InetSocketAddress getAddrNext(final UUID downStreamId) {

            if (downStreamId == null)
                return null;

            final S service = member.getService(downStreamId);

            try {

                final InetSocketAddress addrNext = service.getWritePipelineAddr();

                return addrNext;
                
            } catch (IOException e) {

                throw new RuntimeException(e);

            }

        }

        /**
         * Tear down any state associated with the {@link QuorumPipelineImpl}. This
         * implementation tears down the send/receive service and releases the
         * receive buffer.
         */
        private void tearDown() {
            if (log.isInfoEnabled())
                log.info("");
            lock.lock();
            try {
                /*
                 * Leader tear down.
                 */
                {
                    if (sendService != null) {
                        sendService.terminate();
                        sendService = null;
                    }
                }
                /*
                 * Follower tear down.
                 */
                {
                    if (receiveService != null) {
                        receiveService.terminate();
                        try {
                            receiveService.awaitShutdown();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } finally {
                            receiveService = null;
                        }
                    }
                    if (receiveBuffer != null) {
                        try {
                            /*
                             * Release the buffer back to the pool.
                             */
                            receiveBuffer.release();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } finally {
                            receiveBuffer = null;
                        }
                    }
                }
                // clear cache.
                pipelineStateRef.set(null);
            } finally {
                lock.unlock();
            }
        }

        /**
         * Populate or clear the {@link #pipelineState} cache.
         * <p>
         * Note: The only times we need to populate the {@link #pipelineState} are
         * in response to a {@link #pipelineChange(UUID, UUID)} event or in response
         * to message a {@link #pipelineElectedLeader()} event.
         * 
         * @param downStreamId
         *            The downstream service {@link UUID}.
         */
        private void cachePipelineState(final UUID downStreamId) {
            
            if (downStreamId == null) {
            
                pipelineStateRef.set(null);
                
                return;
            
            }

            final S nextService = member.getService(downStreamId);
            
            final PipelineState<S> pipelineState = new PipelineState<S>();
            
            try {

                pipelineState.addr = nextService.getWritePipelineAddr();
                
            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }
            
            pipelineState.service = nextService;
            
            pipelineStateRef.set(pipelineState);
            
        }
        
        /**
         * Setup the send service.
         */
        private void setUpSendService() {
            if (log.isInfoEnabled())
                log.info("");
            lock.lock();
            try {
                // Allocate the send service.
                sendService = new HASendService();
                /*
                 * The service downstream from this service.
                 * 
                 * Note: The downstream service in the pipeline is not available
                 * when the first service adds itself to the pipeline. In those
                 * cases the pipelineChange() event is used to update the
                 * HASendService to send to the downstream service.
                 * 
                 * Note: When we handle a pipelineLeaderElected() message the
                 * downstream service MAY already be available, which is why we
                 * handle downstreamId != null conditionally.
                 */
                final UUID downstreamId = member.getDownstreamServiceId();
                if (downstreamId != null) {
                    // The address of the next service in the pipeline.
                    final InetSocketAddress addrNext = member.getService(
                            downstreamId).getWritePipelineAddr();
                    // Start the send service.
                    sendService.start(addrNext);
                }
                // populate and/or clear the cache.
                cachePipelineState(downstreamId);
            } catch (Throwable t) {
                try {
                    tearDown();
                } catch (Throwable t2) {
                    log.error(t2, t2);
                }
                throw new RuntimeException(t);
            } finally {
                lock.unlock();
            }
        }

        /**
         * Setup the service to receive pipeline writes and to relay them (if there
         * is a downstream service).
         */
        private void setUpReceiveService() {
            lock.lock();
            try {
                // The downstream service UUID.
                final UUID downstreamId = member.getDownstreamServiceId();
                // Acquire buffer from the pool to receive data.
                try {
                    receiveBuffer = DirectBufferPool.INSTANCE.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                // The address of this service.
                final InetSocketAddress addrSelf = member.getService()
                        .getWritePipelineAddr();
                // Address of the downstream service (if any).
                final InetSocketAddress addrNext = downstreamId == null ? null
                        : member.getService(downstreamId).getWritePipelineAddr();
                // Setup the receive service.
                receiveService = new HAReceiveService<HAMessageWrapper>(addrSelf,
                        addrNext, new IHAReceiveCallback<HAMessageWrapper>() {
                            @Override
                            public void callback(final HAMessageWrapper msg,
                                    final ByteBuffer data) throws Exception {
                                // delegate handling of write cache blocks.
                                handleReplicatedWrite(msg.getHASyncRequest(),
                                        (IHAWriteMessage) msg
                                                .getHAWriteMessage(), data);
                            }
                            @Override
                            public void incReceive(final HAMessageWrapper msg,
                                    final int nreads, final int rdlen,
                                    final int rem) throws Exception {
                                // delegate handling of incremental receive notify.
                                QuorumPipelineImpl.this.incReceive(//
                                        msg.getHASyncRequest(),
                                        (IHAWriteMessage) msg.getHAWriteMessage(), //
                                        nreads, rdlen, rem);
                            }
                        });
                // Start the receive service - will not return until service is
                // running
                receiveService.start();
            } catch (Throwable t) {
                /*
                 * Always tear down if there was a setup problem to avoid leaking
                 * threads or a native ByteBuffer.
                 */
                try {
                    tearDown();
                } catch (Throwable t2) {
                    log.error(t2, t2);
                } finally {
                    log.error(t, t);
                }
                throw new RuntimeException(t);
            } finally {
                lock.unlock();
            }
        }

    };
    
    /**
     * Acquire {@link #lock} and {@link #dispatchEvents()}.
     */
    private void lock() {
        boolean ok = false;
        this.lock.lock();
        try {
            innerEventHandler.dispatchEvents();// have lock, dispatch events.
            ok = true; // success.
        } finally {
            if (!ok) {
                // release lock if there was a problem.
                this.lock.unlock();
            }
        }
    }

    /**
     * Acquire {@link #lock} and {@link #dispatchEvents()}.
     */
    private void lockInterruptibly() throws InterruptedException {
        boolean ok = false;
        lock.lockInterruptibly();
        try {
            innerEventHandler.dispatchEvents(); // have lock, dispatch events.
            ok = true; // success.
        } finally {
            if (!ok) {
                // release lock if there was a problem.
                this.lock.unlock();
            }
        }
    }

    /**
     * {@link #dispatchEvents()} and release {@link #lock}.
     */
    private void unlock() {
        try {
            innerEventHandler.dispatchEvents();
        } finally {
            this.lock.unlock();
        }
    }
   
    public QuorumPipelineImpl(final QuorumMember<S> member) {
        
        if (member == null)
            throw new IllegalArgumentException();

        this.member = member;

        this.serviceId = member.getServiceId();

    }

    /**
     * Extended to invoke {@link #tearDown()} in order to guarantee the eventual
     * release of the {@link #receiveBuffer} and the shutdown of the
     * {@link #sendService} or {@link #receiveService}.
     */
    @Override
    protected void finalize() throws Throwable {

        innerEventHandler.tearDown();

        super.finalize();

    }

    /**
     * Return the index at which the given serviceId appears in the array of
     * serviceIds.
     * 
     * @param serviceId
     *            The {@link UUID} of some quorum member.
     * @param a
     *            An array of service {@link UUID}s.
     * 
     * @return The index of the service in the array -or- <code>-1</code> if the
     *         service does not appear in the array.
     */
    static private int getIndex(final UUID serviceId, final UUID[] a) {

        if (serviceId == null)
            throw new IllegalArgumentException();

        for (int i = 0; i < a.length; i++) {

            if (serviceId.equals(a[i])) {

                return i;

            }
        }

        return -1;

    }

    /**
     * Return the NIO buffer used to receive payloads written on the HA write
     * pipeline.
     * 
     * @return The buffer -or- <code>null</code> if the pipeline has been torn
     *         down or if this is the leader.
     */
    private ByteBuffer getReceiveBuffer() {

        if (!lock.isHeldByCurrentThread()) {

            // The caller MUST be holding the lock.
            throw new IllegalMonitorStateException();

        }

        // trinary pattern is safe while thread has lock.
        return receiveBuffer == null ? null : receiveBuffer.buffer();

    }

    /**
     * Return the {@link HAReceiveService} used to receive payloads written on
     * the HA write pipeline.
     * 
     * @return The buffer -or- <code>null</code> if the pipeline has been torn
     *         down or if this is the leader.
     */
    private HAReceiveService<HAMessageWrapper> getHAReceiveService() {

        if (!lock.isHeldByCurrentThread()) {

            // The caller MUST be holding the lock.
            throw new IllegalMonitorStateException();

        }

        return receiveService;
        
    }

    /**
     * Return the {@link HASendService} used to write payloads on the HA write
     * pipeline.
     * 
     * @return The {@link HASendService} -or- <code>null</code> if the pipeline
     *         has been torn down.
     */
    private HASendService getHASendService() {
        
        if (!lock.isHeldByCurrentThread()) {

            // The caller MUST be holding the lock.
            throw new IllegalMonitorStateException();

        }

        return sendService;
        
    }

    /*
     * QuorumStateChangeListener 
     * 
     * Note: This interface is delegated using a queue.  The queue allows
     * the processing of the events to be deferred until the appropriate
     * lock is held.  This prevents contention for the lock and avoids
     * lock ordering problems such as described at [1].
     * 
     * @see InnerEventHandler
     */
    
    @Override
    public void pipelineAdd() {

        innerEventHandler
                .queue(new QCE(QuorumStateChangeEventEnum.PIPELINE_ADD));

    }

    @Override
    public void pipelineElectedLeader() {

        innerEventHandler.queue(new QCE(
                QuorumStateChangeEventEnum.PIPELINE_ELECTED_LEADER));

    }

    @Override
    public void pipelineRemove() {

        innerEventHandler.queue(new QCE(
                QuorumStateChangeEventEnum.PIPELINE_REMOVE));

    }

    @Override
    public void pipelineChange(final UUID oldDownStreamId,
            final UUID newDownStreamId) {

        innerEventHandler
                .queue(new QCE(QuorumStateChangeEventEnum.PIPELINE_CHANGE,
                        new UUID[] { oldDownStreamId, newDownStreamId },
                        null/* lastCommitTimeConsensus */, null/* token */,
                        null/* leaderId */));

    }

    @Override
    public void pipelineUpstreamChange() {

        innerEventHandler.queue(new QCE(
                QuorumStateChangeEventEnum.PIPELINE_UPSTREAM_CHANGE));

    }

    @Override
    public void memberAdd() {

        innerEventHandler.queue(new QCE(QuorumStateChangeEventEnum.MEMBER_ADD));

    }

    @Override
    public void memberRemove() {

        innerEventHandler.queue(new QCE(
                QuorumStateChangeEventEnum.MEMBER_REMOVE));

    }

    @Override
    public void consensus(final long lastCommitTime) {

        innerEventHandler.queue(new QCE(QuorumStateChangeEventEnum.CONSENSUS,
                null/* downstreamIds */,
                lastCommitTime/* lastCommitTimeConsensus */, null/* token */,
                null/* leaderId */));

    }

    @Override
    public void lostConsensus() {

        innerEventHandler.queue(new QCE(
                QuorumStateChangeEventEnum.LOST_CONSENSUS));

    }

    @Override
    public void serviceJoin() {

        innerEventHandler
                .queue(new QCE(QuorumStateChangeEventEnum.SERVICE_JOIN));

    }

    @Override
    public void serviceLeave() {

        innerEventHandler.queue(new QCE(
                QuorumStateChangeEventEnum.SERVICE_LEAVE));

    }

    @Override
    public void quorumMeet(final long token, final UUID leaderId) {

        innerEventHandler.queue(new QCE(QuorumStateChangeEventEnum.QUORUM_MEET,
                null/* downstreamIds */, null/* lastCommitTimeConsensus */,
                token, leaderId));

    }

    @Override
    public void quorumBreak() {

        innerEventHandler
                .queue(new QCE(QuorumStateChangeEventEnum.QUORUM_BREAK));

    }
    
    /*
     * End of QuorumStateChangeListener.
     */

    private IHASendState newSendState() {

        final Quorum<?, ?> quorum = member.getQuorum();

        final IHASendState snd = new HASendState(messageId.incrementAndGet(),
                serviceId/* originalSenderId */, serviceId/* senderId */,
                quorum.token(), quorum.replicationFactor());

        return snd;

    }
    
    /*
     * This is the leader, so send() the buffer.
     */
    @Override
    public Future<Void> replicate(final IHASyncRequest req,
            final IHAWriteMessage msg, final ByteBuffer b) throws IOException {

        final RunnableFuture<Void> ft;

        lock();
        try {

            ft = new FutureTask<Void>(new RobustReplicateTask(req,
                    newSendState(), msg, b));

        } finally {

            unlock();

        }

        // Submit Future for execution (outside of the lock).
        member.getExecutor().execute(ft);

        // Return Future. Caller must wait on the Future.
        return ft;

    }

    /**
     * Task robustly replicates an {@link IHAWriteMessage} and the associated
     * payload. 
     */
    private class RobustReplicateTask implements Callable<Void> {

        /**
         * An historical message is indicated when the {@link IHASyncRequest} is
         * non-<code>null</code>.
         */
        private final IHASyncRequest req;
        
        /**
         * Metadata about the state of the sender for this message.
         */
        private final IHASendState snd;
        
        /**
         * The {@link IHAWriteMessage}.
         */
        private final IHAWriteMessage msg;
        
        /**
         * The associated payload.
         */
        private final ByteBuffer b;

        /**
         * The token for the leader. The service that initiates the replication
         * of a message MUST be the leader for this token.
         * <p>
         * The token is either taken from the {@link IHAWriteMessage} (if this
         * is a live write) or from the current {@link Quorum#token()}.
         * <p>
         * Either way, we verify that this service is (and remains) the leader
         * for that token throughout the {@link RobustReplicateTask}.
         */
        private final long quorumToken;

        public RobustReplicateTask(final IHASyncRequest req,
                final IHASendState snd, final IHAWriteMessage msg,
                final ByteBuffer b) {
            
            // Note: [req] MAY be null.

            if (snd == null)
                throw new IllegalArgumentException();

            if (msg == null)
                throw new IllegalArgumentException();
            
            if (b == null)
                throw new IllegalArgumentException();

            this.req = req;

            this.snd = snd;
            
            this.msg = msg;
            
            this.b = b;

            if (b.remaining() == 0) {
             
                // Empty buffer.
                
                throw new IllegalStateException("Empty buffer: req=" + req
                        + ", msg=" + msg + ", buffer=" + b);
                
            }

            if (req == null) {
            
                /*
                 * Live message.
                 * 
                 * Use the quorum token on the message. It was put there by the
                 * WriteCacheService. This allows us to ensure that the qourum
                 * token remains valid for all messages replicated by the
                 * leader.
                 */

                quorumToken = msg.getQuorumToken();

                // Must be the leader for that token.
                member.assertLeader(quorumToken);

            } else {
                
                /*
                 * Historical message.
                 */

                // Use the current quorum token.
                quorumToken = member.getQuorum().token();
                
                // Must be the leader for that token.
                member.assertLeader(quorumToken);
                
            }
            
        }

        /**
         * This collects the assertion(s) that we make for the service that is
         * attempting to robustly replicate a write into a single method. This
         * was done in order to concentrate any conditional logic and design
         * rationale into a single method.
         * <p>
         * Note: IFF we allow non-leaders to replicate HALog messages then this
         * assert MUST be changed to verify that the quorum token remains valid
         * and that this service remains joined with the met quorum, i.e.,
         * 
         * <pre>
         * if (!quorum.isJoined(token))
         *     throw new QuorumException();
         * </pre>
         */
        private void assertQuorumState() throws QuorumException {

            // Must be the leader for that token.
            member.assertLeader(quorumToken);

//            if (req == null) {
//
//                /*
//                 * This service must be the leader since this is a LIVE
//                 * write cache message (versus a historical message that is
//                 * being replayed).
//                 * 
//                 * Note: The [quorumToken] is from the message IFF this is a
//                 * live message and is otherwise the current quorum token.
//                 */
//                member.assertLeader(quorumToken);
//
//            }

        }

        /**
         * Robust replication of a write cache block along the pipeline.
         * <p>
         * Note: {@link Future} for {@link #call()} is [WCS.remoteWriteFuture].
         * <p>
         * Note: In order for replication from the leader to be robust while
         * still permitting the WCS to shutdown, we need to distinguish two
         * different ways in which replication can be interrupted/cancelled:
         * <p>
         * 1. WCS.close()/reset() => throw;
         * <p>
         * 2. pipelineChange() => retrySend()
         * <p>
         * If the WCS is being shutdown, then we MUST NOT attempt to cure the
         * exception thrown out of innerReplicate(). This will show up as an
         * {@link InterruptedException} if the interrupt is encountered in this
         * thread (call()).
         * <p>
         * 
         * If there is a {@link QuorumPipelineImpl#pipelineChange(UUID, UUID)},
         * then that method will cause {@link SendBufferTask} and/or
         * {@link ReceiveAndReplicateTask} to be cancelled. {@link Future#get()}
         * for those tasks thus can report a {@link CancellationException}.
         * <p>
         * If we fail to distinguish these cases, then a pipelineChange() can
         * cause the {@link RobustReplicateTask} to fail, which is precisely
         * what it should not do. Instead the pipeline should be re-established
         * and the writes replicated by the leader along the new pipeline order.
         * <p>
         * The following stack traces are illustrative of a problem when the
         * pipeline is [A,C] and [B] joins while a write is being replicated
         * from [A] to [C]. The join of [B] triggers pipelineChange() which
         * causes the {@link SendBufferTask} to be cancelled. The untrapped
         * {@link CancellationException} propagates from this method to
         * WCS.WriteTask.writeCacheBlock() to fail, failing the transaction. The
         * correct behavior in this case is to enter retrySend() to cure the
         * pipelineChange().
         * 
         * <pre>
         * Leader(A):
         * ERROR: 10:47:39,027 29698      com.bigdata.rwstore.RWStore$11 com.bigdata.io.writecache.WriteCacheService$WriteTask.call(WriteCacheService.java:937): java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.util.concurrent.CancellationException
         * java.util.concurrent.ExecutionException: java.lang.RuntimeException: java.util.concurrent.CancellationException
         *     at java.util.concurrent.FutureTask$Sync.innerGet(FutureTask.java:252)
         *     at java.util.concurrent.FutureTask.get(FutureTask.java:111)
         *     at com.bigdata.io.writecache.WriteCacheService$WriteTask.writeCacheBlock(WriteCacheService.java:1466)
         *     at com.bigdata.io.writecache.WriteCacheService$WriteTask.doRun(WriteCacheService.java:1015)
         *     at com.bigdata.io.writecache.WriteCacheService$WriteTask.call(WriteCacheService.java:884)
         *     at com.bigdata.io.writecache.WriteCacheService$WriteTask.call(WriteCacheService.java:1)
         *     at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:334)
         *     at java.util.concurrent.FutureTask.run(FutureTask.java:166)
         *     at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1110)
         *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:603)
         *     at java.lang.Thread.run(Thread.java:722)
         * Caused by: java.lang.RuntimeException: java.util.concurrent.CancellationException
         *     at com.bigdata.ha.QuorumPipelineImpl$RobustReplicateTask.call(QuorumPipelineImpl.java:912)
         *     at com.bigdata.ha.QuorumPipelineImpl$RobustReplicateTask.call(QuorumPipelineImpl.java:1)
         *     ... 5 more
         * Caused by: java.util.concurrent.CancellationException
         *     at java.util.concurrent.FutureTask$Sync.innerGet(FutureTask.java:250)
         *     at java.util.concurrent.FutureTask.get(FutureTask.java:111)
         *     at com.bigdata.service.proxy.ThickFuture.<init>(ThickFuture.java:66)
         *     at com.bigdata.journal.jini.ha.HAJournal$HAGlueService.getProxy(HAJournal.java:1539)
         *     at com.bigdata.journal.AbstractJournal$BasicHA.getProxy(AbstractJournal.java:5976)
         *     at com.bigdata.journal.AbstractJournal$BasicHA.receiveAndReplicate(AbstractJournal.java:6718)
         *     at com.bigdata.journal.jini.ha.HAJournalTest$HAGlueTestImpl.receiveAndReplicate(HAJournalTest.java:757)
         *     at sun.reflect.GeneratedMethodAccessor7.invoke(Unknown Source)
         *     at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
         *     at java.lang.reflect.Method.invoke(Method.java:601)
         *     at net.jini.jeri.BasicInvocationDispatcher.invoke(BasicInvocationDispatcher.java:1126)
         *     at net.jini.jeri.BasicInvocationDispatcher.dispatch(BasicInvocationDispatcher.java:608)
         *     at com.sun.jini.jeri.internal.runtime.Target$2.run(Target.java:487)
         *     at net.jini.export.ServerContext.doWithServerContext(ServerContext.java:103)
         *     at com.sun.jini.jeri.internal.runtime.Target.dispatch(Target.java:484)
         *     at com.sun.jini.jeri.internal.runtime.Target.access$000(Target.java:57)
         *     at com.sun.jini.jeri.internal.runtime.Target$1.run(Target.java:464)
         *     at java.security.AccessController.doPrivileged(Native Method)
         *     at com.sun.jini.jeri.internal.runtime.Target.dispatch(Target.java:461)
         *     at com.sun.jini.jeri.internal.runtime.Target.dispatch(Target.java:426)
         *     at com.sun.jini.jeri.internal.runtime.DgcRequestDispatcher.dispatch(DgcRequestDispatcher.java:210)
         *     at net.jini.jeri.connection.ServerConnectionManager$Dispatcher.dispatch(ServerConnectionManager.java:147)
         *     at com.sun.jini.jeri.internal.mux.MuxServer$1$1.run(MuxServer.java:244)
         *     at java.security.AccessController.doPrivileged(Native Method)
         *     at com.sun.jini.jeri.internal.mux.MuxServer$1.run(MuxServer.java:241)
         *     at com.sun.jini.thread.ThreadPool$Worker.run(ThreadPool.java:136)
         *     at java.lang.Thread.run(Thread.java:722)
         *     at com.sun.jini.jeri.internal.runtime.Util.__________EXCEPTION_RECEIVED_FROM_SERVER__________(Util.java:108)
         *     at com.sun.jini.jeri.internal.runtime.Util.exceptionReceivedFromServer(Util.java:101)
         *     at net.jini.jeri.BasicInvocationHandler.unmarshalThrow(BasicInvocationHandler.java:1303)
         *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethodOnce(BasicInvocationHandler.java:832)
         *     at net.jini.jeri.BasicInvocationHandler.invokeRemoteMethod(BasicInvocationHandler.java:659)
         *     at net.jini.jeri.BasicInvocationHandler.invoke(BasicInvocationHandler.java:528)
         *     at $Proxy2.receiveAndReplicate(Unknown Source)
         *     at com.bigdata.ha.QuorumPipelineImpl$SendBufferTask.doRunWithLock(QuorumPipelineImpl.java:1127)
         *     at com.bigdata.ha.QuorumPipelineImpl$SendBufferTask.call(QuorumPipelineImpl.java:1105)
         *     at com.bigdata.ha.QuorumPipelineImpl$RobustReplicateTask.innerReplicate(QuorumPipelineImpl.java:967)
         *     at com.bigdata.ha.QuorumPipelineImpl$RobustReplicateTask.call(QuorumPipelineImpl.java:906)
         *     ... 6 more
         * </pre>
         * 
         * <pre>
         * First follower (C):
         * WARN : 10:47:39,002 14879      com.bigdata.ha.pipeline.HAReceiveService@705072408{addrSelf=localhost/127.0.0.1:9092} com.bigdata.ha.pipeline.HAReceiveService.runNoBlock(HAReceiveService.java:547): java.util.concurrent.CancellationException
         * java.util.concurrent.CancellationException
         *     at java.util.concurrent.FutureTask$Sync.innerGet(FutureTask.java:250)
         *     at java.util.concurrent.FutureTask.get(FutureTask.java:111)
         *     at com.bigdata.ha.pipeline.HAReceiveService.runNoBlock(HAReceiveService.java:545)
         *     at com.bigdata.ha.pipeline.HAReceiveService.run(HAReceiveService.java:431)
         * </pre>
         */
        @Override
        public Void call() throws Exception {

            final long beginNanos = System.nanoTime();
            
            /*
             * Note: This is tested outside of the try/catch. Do NOT retry if
             * the quorum state has become invalid.
             */
            assertQuorumState();

            try {

                innerReplicate(0/* retryCount */);
                
            } catch (Throwable t) {

                // Note: Also see retrySend()'s catch block.
                if (InnerCause.isInnerCause(t, InterruptedException.class)
//               || InnerCause.isInnerCause(t, CancellationException.class)
                        ) {

                    throw new RuntimeException(t);
                    
                }
                
                // Log initial error.
                log.error(t, t);

                if (!retrySend()) {

                    final long elapsedNanos = System.nanoTime() - beginNanos;
                    
                    // Rethrow the original exception.
                    throw new RuntimeException(
                            "Giving up. Could not send after "
                                    + TimeUnit.NANOSECONDS.toMillis(elapsedNanos)
                                    + "ms : " + t, t);

                }
                
            }

            return null;
            
        }

        /**
         * Replicate from the leader to the first follower. Each non-final
         * follower will receiveAndReplicate the write cache buffer. The last
         * follower will receive the buffer.
         * 
         * @param retryCount
         *            The #of attempts and ZERO (0) if this is the first
         *            attempt.
         *            
         * @throws Exception
         */
        private void innerReplicate(final int retryCount) throws Exception {

            lockInterruptibly();

            try {

                if (log.isInfoEnabled() || retryCount > 0) {
                    final String msg2 = "Leader will send: " + b.remaining()
                            + " bytes, retryCount=" + retryCount + ", req="
                            + req + ", msg=" + msg;
                    if (retryCount > 0)
                        log.warn(msg2);
                    else
                        log.info(msg2);
                }

                // retest while holding lock before sending the message.
                assertQuorumState();
                
                final PipelineState<S> downstream = pipelineStateRef.get();

                final HASendService sendService = getHASendService();

                final ByteBuffer b = this.b.duplicate();

                new SendBufferTask<S>(member, quorumToken, req, snd, msg, b,
                        downstream, sendService, sendLock).call();

                return;

            } finally {

                unlock();

            }

        } // call()

        /**
         * Robust retransmit of the current cache block. This method is designed to
         * handle several kinds of recoverable errors, including:
         * <ul>
         * <li>downstream service leaves the pipeline</li>
         * <li>intermittent failure sending the RMI message</li>
         * <li>intermittent failure sending the payload</li>
         * </ul>
         * The basic pattern is that it will retry the operation a few times to see
         * if there is a repeatable error. Each time it attempts the operation it
         * will discover the current downstream serviceId and verify that the quorum
         * is still valid. Each error (including the first) is logged. If the
         * operation fails, the original error is rethrown. If the operation
         * succeeds, then the cache block was successfully transmitted to the
         * current downstream service and this method returns without error.
         * 
         * @throws InterruptedException 
         */
        private boolean retrySend() throws InterruptedException {

            final long beginNanos = System.nanoTime();
            final long nanos = getRetrySendTimeoutNanos();
            final long retrySleepNanos = TimeUnit.MILLISECONDS.toNanos(RETRY_SLEEP);
            
            int tryCount = 1; // already retried once.

            // now try some more times.
            while (true) {

                long remaining = nanos - (System.nanoTime() - beginNanos);

                if (remaining <= retrySleepNanos)
                    break;
                
                // Sleep before each retry (including the first).
                Thread.sleep(RETRY_SLEEP/* ms */);

                remaining = nanos - (System.nanoTime() - beginNanos);
                
                /*
                 * Note: Tested OUTSIDE of the try/catch so a quorum break will
                 * immediately stop the retry behavior.
                 */
                assertQuorumState();

                try {

                    // send to 1st follower.
                    innerReplicate(tryCount++);

                    // Success.
                    return true;
                    
                } catch (Throwable t) {
                    
                    // Note: Also see call()'s catch block.
                    if (InnerCause.isInnerCause(t, InterruptedException.class)
//                   || InnerCause.isInnerCause(t, CancellationException.class)
                     ) {
                        
                        throw new RuntimeException( t );
                        
                    }
                    
                    // log and retry.
                    log.error(
                            "retry="
                                    + tryCount
                                    + ", elapsed="
                                    + TimeUnit.NANOSECONDS.toMillis(System
                                            .nanoTime() - beginNanos) + "ms : "
                                    + t, t);

                    continue;
                    
                }

            }

            // Send was not successful.
            return false;
            
        } // retrySend()

    } // class RobustReplicateTask
    
//    /**
//     * The logic needs to support the asynchronous termination of the
//     * {@link Future} that is responsible for replicating the {@link WriteCache}
//     * block, which is why the API exposes the means to inform the caller about
//     * that {@link Future}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     */
//    public interface IRetrySendCallback {
//        /**
//         * 
//         * @param remoteFuture
//         */
//        void notifyRemoteFuture(final Future<Void> remoteFuture);
//    }
    
    /**
     * Task to send() a buffer to the follower.
     */
    static private class SendBufferTask<S extends HAPipelineGlue> implements
            Callable<Void> {

        private final QuorumMember<S> member;
        private final long token; // member MUST remain leader for token.
        private final IHASyncRequest req;
        private final IHASendState snd;
        private final IHAWriteMessage msg;
        private final ByteBuffer b;
        private final PipelineState<S> downstream;
        private final HASendService sendService;
        private final Lock sendLock;

        public SendBufferTask(final QuorumMember<S> member, final long token,
                final IHASyncRequest req, final IHASendState snd,
                final IHAWriteMessage msg, final ByteBuffer b,
                final PipelineState<S> downstream,
                final HASendService sendService, final Lock sendLock) {

            this.member = member;
            this.token = token; 
            this.req = req; // Note: MAY be null.
            this.snd = snd;
            this.msg = msg;
            this.b = b;
            this.downstream = downstream;
            this.sendService = sendService;
            this.sendLock = sendLock;

        }

        @Override
        public Void call() throws Exception {

            /*
             * Lock ensures that we do not have more than one request on the
             * write pipeline at a time.
             */

            sendLock.lock();

            try {

                doRunWithLock();
                
                return null;
                
            } finally {
                
                sendLock.unlock();
                
            }

        }
        
        private void doRunWithLock() throws InterruptedException,
                ExecutionException, IOException {

            // Get Future for send() outcome on local service.
            final Future<Void> futSnd = sendService.send(b, snd.getMarker());
            try {
                try {

                    // Get Future for receive outcome on the remote service
                    // (RMI).
                    final Future<Void> futRec;
                    try {
                        futRec = downstream.service.receiveAndReplicate(req,
                                snd, msg);
                    } catch (IOException ex) { // RMI error.
                        throw new ImmediateDownstreamReplicationException(ex);
                    }

                    try {

                        /*
                         * Await the Futures, but spend more time waiting on the
                         * local Future and only check the remote Future every
                         * second. Timeouts are ignored during this loop.
                         */
                        while (!futSnd.isDone() || !futRec.isDone()) {
                            /*
                             * Make sure leader's quorum token remains valid for
                             * ALL writes.
                             */
                            member.assertLeader(token);
                            try {
                                futSnd.get(1L, TimeUnit.SECONDS);
                            } catch (TimeoutException ignore) {
                            }
                            try {
                                futRec.get(10L, TimeUnit.MILLISECONDS);
                            } catch (TimeoutException ignore) {
                            }
                        }
                        
                        // Note: Both futures are DONE at this point!
                        futSnd.get();
                        futRec.get();

                    } finally {
                        if (!futRec.isDone()) {
                            // cancel remote Future unless done.
                            futRec.cancel(true/* mayInterruptIfRunning */);
                        }
                    }

                } finally {
                    // cancel the local Future.
                    futSnd.cancel(true/* mayInterruptIfRunning */);
                }
            } catch (Throwable t) {
                launderPipelineException(true/* isLeader */, member, t);
            }
        }
        
    }

    /**
     * Launder an exception thrown during pipeline replication.  
     * @param isLeader
     * @param member
     * @param t
     */
    static private void launderPipelineException(final boolean isLeader,
            final QuorumMember<?> member, final Throwable t) {

        log.warn("isLeader=" + isLeader + ", t=" + t, t);
        
        /*
         * When non-null, some service downstream of this service had a problem
         * replicating to a follower.
         */
        final PipelineImmediateDownstreamReplicationException remoteCause = (PipelineImmediateDownstreamReplicationException) InnerCause.getInnerCause(t,
            PipelineImmediateDownstreamReplicationException.class);

        /*
         * When non-null, this service has a problem with replication to its
         * immediate follower.
         * 
         * Note: if [remoteCause!=null], then we DO NOT look for a direct cause
         * (since there will be one wrapped up in the exception trace for some
         * remote service rather than for this service).
         */
        final ImmediateDownstreamReplicationException directCause = remoteCause == null ? (ImmediateDownstreamReplicationException) InnerCause
                .getInnerCause(t,
                        ImmediateDownstreamReplicationException.class)
                : null;

        final UUID thisService = member.getServiceId();

        final UUID[] priorAndNext = member.getQuorum().getPipelinePriorAndNext(
                member.getServiceId());

        if (isLeader) {
            
            try {
                
                if (directCause != null) {

                    member.getActor().forceRemoveService(priorAndNext[1]);

                } else if (remoteCause != null) {

                    final UUID problemService = remoteCause.getProblemServiceId();

                    member.getActor().forceRemoveService(problemService);

                } else {
                    
                    // Do not remove anybody.
                    
                }
                
            } catch (Exception e) {
                
                log.error("Problem on node removal", e);

                throw new RuntimeException(e);
                
            }

        }

        if (directCause != null) {

            throw new PipelineImmediateDownstreamReplicationException(
                    thisService, priorAndNext, t);
            
        } else if (remoteCause != null) {
            
            throw new NestedPipelineException(t);
            
        } else {
            
            throw new RuntimeException(t);
            
        }
    
    }
    
    /**
     * Lock used to ensure that at most one message is being sent along the
     * write pipeline at a time.
     */
    private final Lock sendLock = new ReentrantLock();

    @Override
    public Future<Void> receiveAndReplicate(final IHASyncRequest req,
            final IHASendState snd, final IHAWriteMessage msg)
            throws IOException {

        /*
         * FIXME We should probably pass the quorum token through from the
         * leader for ALL replicated writes [this is now done by the
         * IHASendState but the code is not really using that data yet]. This
         * uses the leader's quorum token when it is available (for a live
         * write) and otherwise uses the current quorum token (for historical
         * writes, since we are not providing the leader's token in this case).
         */
        final long token = req == null ? msg.getQuorumToken() : member
                .getQuorum().token();

        final RunnableFuture<Void> ft;

        lock();

        try {

            // Must be valid quorum.
            member.getQuorum().assertQuorum(token);

            if (receiveBuffer == null) {

                /*
                 * The quorum broke and the receive buffer was cleared or
                 * possibly we have become a leader (a distinct test since
                 * otherwise we can hit an NPE on the receiveBuffer).
                 */

                throw new QuorumException();

            }
            
            final PipelineState<S> downstream = pipelineStateRef.get();

            if (log.isTraceEnabled())
                log.trace("Will receive "
                        + ((downstream != null) ? " and replicate" : "") + ": msg="
                        + msg);
            
            final ByteBuffer b = getReceiveBuffer();
            
            final HAReceiveService<HAMessageWrapper> receiveService = getHAReceiveService();

            if (downstream == null) {

                /*
                 * This is the last service in the write pipeline, so just
                 * receive the buffer.
                 * 
                 * Note: The receive service is executing this Future locally on
                 * this host. However, we still want the receiveData() method to
                 * run while we are not holding the [lock] so we wrap it up as a
                 * task and submit it.
                 */

                ft = new FutureTask<Void>(new ReceiveTask<S>(member, token,
                        req, snd, msg, b, receiveService));
                
//                try {
//
//                    // wrap the messages together.
//                    final HAMessageWrapper wrappedMsg = new HAMessageWrapper(
//                            req, msg);
//                    
//                    // receive.
//                    return receiveService.receiveData(wrappedMsg, b);
//
//                } catch (InterruptedException e) {
//
//                    throw new RuntimeException(e);
//
//                }

            } else {

                /*
                 * A service in the middle of the write pipeline (not the first
                 * and not the last).
                 */

                ft = new FutureTask<Void>(new ReceiveAndReplicateTask<S>(
                        member, token, req, snd, msg, b, downstream,
                        receiveService));

            }

        } finally {
            
            unlock();
            
        }

        // Execute the FutureTask (w/o the lock).
        member.getExecutor().execute(ft);

        return ft;

    }

    /**
     * Task sets up the {@link Future} for the receive on the last follower.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @param <S>
     */
    private static class ReceiveTask<S extends HAPipelineGlue> implements
            Callable<Void> {

        private final QuorumMember<S> member;
        private final long token;
        private final IHASyncRequest req;
        private final IHASendState snd;
        private final IHAWriteMessage msg;
        private final ByteBuffer b;
        private final HAReceiveService<HAMessageWrapper> receiveService;

        public ReceiveTask(final QuorumMember<S> member,
                final long token, 
                final IHASyncRequest req,
                final IHASendState snd,
                final IHAWriteMessage msg, final ByteBuffer b,
                final HAReceiveService<HAMessageWrapper> receiveService
                ) {

            this.member = member;
            this.token = token;
            this.req = req; // Note: MAY be null.
            this.snd = snd;
            this.msg = msg;
            this.b = b;
            this.receiveService = receiveService;
        }
        
        @Override
        public Void call() throws Exception {

            // wrap the messages together. 
            final HAMessageWrapper wrappedMsg = new HAMessageWrapper(
                    req, snd, msg);

            // Get Future for receive() outcome on local service.
            final Future<Void> futRec = receiveService.receiveData(wrappedMsg,
                    b);

            try {

                // Await outcome while monitoring the quorum token.
                while (true) {
                    try {
                        // Verify token remains valid.
                        member.getQuorum().assertQuorum(token);
                        // Await the future.
                        return futRec.get(1000, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException ex) {
                        // Timeout. Ignore and retry loop.
                        Thread.sleep(100/* ms */);
                        continue;
                    }
                }

            } finally {

                // cancel the local Future.
                futRec.cancel(true/*mayInterruptIfRunning*/);
                
            }
            
        }

    }
    
    /**
     * A service in the middle of the write pipeline (not the first and not the
     * last).
     */
    private static class ReceiveAndReplicateTask<S extends HAPipelineGlue>
            implements Callable<Void> {
        
        private final QuorumMember<S> member;
        private final long token;
        private final IHASyncRequest req;
        private final IHASendState snd;
        private final IHAWriteMessage msg;
        private final ByteBuffer b;
        private final PipelineState<S> downstream;
        private final HAReceiveService<HAMessageWrapper> receiveService;

        public ReceiveAndReplicateTask(final QuorumMember<S> member,
                final long token, final IHASyncRequest req,
                final IHASendState snd,
                final IHAWriteMessage msg, final ByteBuffer b,
                final PipelineState<S> downstream,
                final HAReceiveService<HAMessageWrapper> receiveService) {

            this.member = member;
            this.token = token;
            this.req = req; // Note: MAY be null.
            this.snd = snd;
            this.msg = msg;
            this.b = b;
            this.downstream = downstream;
            this.receiveService = receiveService;
        }

        @Override
        public Void call() throws Exception {

            // wrap the messages together.
            final HAMessageWrapper wrappedMsg = new HAMessageWrapper(
                    req, snd, msg);

            // Get Future for receive() outcome on local service.
            final Future<Void> futRec = receiveService.receiveData(wrappedMsg,
                    b);

            try {
                try {

                    // Get Future for receive outcome on the remote service
                    // (RMI).
                    final Future<Void> futRep;
                    try {
                        futRep = downstream.service.receiveAndReplicate(req,
                                snd, msg);
                    } catch (IOException ex) { // RMI error.
                        throw new ImmediateDownstreamReplicationException(ex);
                    }

                    try {

                        /*
                         * Await the Futures, but spend more time waiting on the
                         * local Future and only check the remote Future every
                         * second. Timeouts are ignored during this loop.
                         */
                        while (!futRec.isDone() || !futRep.isDone()) {
                            /*
                             * The token must remain valid, even if this service
                             * is not joined with the met quorum. If fact,
                             * services MUST replicate writes regardless of
                             * whether or not they are joined with the met
                             * quorum, but only while there is a met quorum.
                             */
                            member.getQuorum().assertQuorum(token);
                            try {
                                futRec.get(1L, TimeUnit.SECONDS);
                            } catch (TimeoutException ignore) {
                            }
                            try {
                                futRep.get(10L, TimeUnit.MILLISECONDS);
                            } catch (TimeoutException ignore) {
                            }
                        }
                        
                        // Note: Both futures are DONE at this point!
                        futRec.get();
                        futRep.get();

                    } finally {
                        if (!futRep.isDone()) {
                            // cancel remote Future unless done.
                            futRep.cancel(true/* mayInterruptIfRunning */);
                        }
                    }

                } finally {
                    // cancel the local Future.
                    futRec.cancel(true/* mayInterruptIfRunning */);
                }
            } catch (Throwable t) {
                launderPipelineException(false/* isLeader */, member, t);
            }
            // done
            return null;
        }

    }

    /**
     * Core implementation handles the message and payload when received on a
     * service.
     * <p>
     * Note: Replication of the message and payload is handled by the caller.
     * The implementation of this method is NOT responsible for replication.
     * 
     * @param req
     *            The synchronization request (optional). When non-
     *            <code>null</code> the msg and payload are historical data.
     *            When <code>null</code> they are live data.
     * @param msg
     *            Metadata about a buffer containing data replicated to this
     *            node.
     * @param data
     *            The buffer containing the data.
     * 
     * @throws Exception
     */
    abstract protected void handleReplicatedWrite(final IHASyncRequest req,
            final IHAWriteMessage msg, final ByteBuffer data) throws Exception;

    /**
     * Notify that some payload bytes have been incrementally received for an
     * {@link IHAMessage}.
     * 
     * @param msg
     *            The message.
     * @param nreads
     *            The number of reads performed against the upstream socket for
     *            this message.
     * @param rdlen
     *            The number of bytes read from the socket in this read.
     * @param rem
     *            The number of bytes remaining before the payload has been
     *            fully read.
     * 
     * @throws Exception
     */
    abstract protected void incReceive(final IHASyncRequest req,
            final IHAWriteMessage msg, final int nreads, final int rdlen,
            final int rem) throws Exception;

    /**
     * A utility class that bundles together the Internet address and port at which
     * the downstream service will accept and relay cache blocks for the write
     * pipeline and the remote interface which is used to communicate with that
     * service using RMI.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private
    static
    class PipelineState<S extends HAPipelineGlue> implements Externalizable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /**
         * The Internet address and port at which the downstream service will accept
         * and relay cache blocks for the write pipeline.
         */
        public InetSocketAddress addr;

        /**
         * The remote interface for the downstream service which will accept and
         * relay cache blocks from this service.
         * <p>
         * Note: In order for an instance of this class to be serializable, an
         * exported proxy for the {@link HAGlue} object must be used here rather
         * than the local object reference.
         */
        public S service;

        public PipelineState() {

        }

        @SuppressWarnings("unchecked")
        public void readExternal(final ObjectInput in) throws IOException,
                ClassNotFoundException {

            addr = (InetSocketAddress) in.readObject();

            service = (S) in.readObject();

        }

        public void writeExternal(final ObjectOutput out) throws IOException {

            out.writeObject(addr);

            out.writeObject(service);

        }

    }

    /**
     * Called from ErrorTask in HAJournalServer to ensure that events are
     * processed before entering SeekConsensus.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/695">
     * HAJournalServer reports "follower" but is in SeekConsensus and is not
     * participating in commits</a>
     */
	public void processEvents() {
        this.lock.lock();
        try {
            innerEventHandler.dispatchEvents();// have lock, dispatch events.
        } finally {
            this.lock.unlock();
        }

	}
    
}
