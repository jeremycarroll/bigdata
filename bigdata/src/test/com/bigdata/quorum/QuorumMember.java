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
/*
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

import java.rmi.Remote;
import java.util.UUID;

import com.bigdata.journal.ha.QuorumException;

/**
 * A non-remote interface for a service which will participate in a quorum as a
 * member service (as opposed to a client service that merely watches the
 * quorum). The methods on this interface are aware of the service {@link UUID}
 * of the member service and can report on its role and relationships in the
 * {@link Quorum}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumMember<S extends Remote> extends QuorumClient<S> {

    /**
     * The identifier for this service.
     */
    UUID getServiceId();

    /**
     * Return <code>true</code>if the quorum recognizes the service as a member
     * of that quorum. The quorum token is not required for this method because
     * membership status does not change with a quorum meet or break.
     */
    boolean isMember();

    /**
     * Return <code>true</code> if quorum recognizes the service is part of the
     * write pipeline. The quorum token is not required for this method because
     * pipeline status does not change with a quorum meet or break. Once a
     * service is receiving synchronous messages from a {@link Quorum} it will
     * notice when it enters and leaves the pipeline and when the downstream
     * service for this service changes in the quorum.
     */
    boolean isPipelineMember();
    
    /**
     * Return <code>true</code> iff the quorum is highly available and this
     * node is last one in the write pipeline (it will not return true for a
     * singleton quorum where the only node is the master).
     * 
     * @param token
     *            The quorum token for which the request was made.
     */
    boolean isLastInChain(long token);

    /**
     * Return <code>true</code> if the service is joined with the quorum.
     * 
     * @param token
     *            The quorum token for which the request was made.
     */
    boolean isJoinedMember(long token);
    
    /**
     * Return <code>true</code> iff this node is the quorum leader. The
     * quorum leader is the only node which will accept writes. Note that is
     * always <code>true</code> when the {@link #replicationFactor()} is ONE
     * (1).
     * 
     * @param token
     *            The quorum token for which the request was made.
     */
    boolean isLeader(long token);

    /**
     * Return <code>true</code> iff this node is a quorum follower. This is
     * <code>true</code> of all nodes in a {@link Q} except for the leader.
     * 
     * @param token
     *            The quorum token for which the request was made.
     */
    boolean isFollower(long token);

    /**
     * Return metadata used to communicate with the downstream node in the write
     * pipeline. When a quorum is met, the <i>leader</i> is always first in the
     * write pipeline since it is the node which receives writes from clients.
     * When a service joins the write pipeline, it always does so at the end of
     * the chain. Services may enter the write pipeline before joining a quorum
     * in order to synchronize with the quorum. If a service in the middle of
     * the chain leaves the pipeline, then the upstream node will reconfigure
     * and retransmit the current cache block to its new downstream node. This
     * prevent nodes which are "bouncing" during synchronization from causing
     * write sets to be discarded. However, if the leader leaves the write
     * pipeline, then the current token is invalidated and the write set will be
     * discarded.
     * 
     * @param token
     *            The quorum token for which the request was made.
     * 
     * @return The UUID of the downstream service in the write pipeline. This
     *         will return <code>null</code> for services which are not part of
     *         the write pipeline, for the leader, and for the last service in
     *         the write pipeline.
     * 
     * @return <code>null</code> if there is no downstream service.
     * 
     * @throws QuorumException
     *             if the quorum token is no longer valid.
     */
    UUID getDownstreamService(long token);

    /**
     * Invoked when the service is added to the write pipeline. The service
     * always enters at the of the pipeline.
     */
    void pipelineAdd();

    /**
     * Invoked when this service is removed from the write pipeline.
     */
    void pipelineRemove();

    /**
     * Invoked when the downstream service in the write pipeline has changed.
     * Services always enter at the end of the write pipeline, but may be
     * removed at any position in the write pipeline.
     * 
     * @param oldDownstreamId
     *            The {@link UUID} of the service which <em>was</em> downstream
     *            from this service in the write pipeline and <code>null</code>
     *            iff this service was the last service in the pipeline.
     * @param newDownstreamId
     *            The {@link UUID} of the service which <em>is</em> downstream
     *            from this service in the write pipeline and <code>null</code>
     *            iff this service <em>is</em> the last service in the pipeline.
     */
    void pipelineChange(UUID oldDownStreamId,UUID newDownStreamId);
    
    /**
     * Invoked when <em>this</em> quorum member is elected as the quorum leader.
     * This event only occurs when the quorum meets.
     * 
     * @param token
     *            The newly assigned quorum token.
     */
    void electedLeader(long token);

    /**
     * Invoked when <em>this</em> quorum member is elected as a quorum follower.
     * This event occurs both when the quorum meets and when a quorum member is
     * becomes synchronized with and then joins an already met quorum.
     * 
     * @param token
     *            The newly assigned quorum token.
     * 
     *            FIXME Verify that we get this event both on meet and on join
     *            after meet.
     */
    void electedFollower(long token);

    /**
     * Invoked when this service is added as a quorum member.
     */
    void memberAdd();
    
    /**
     * Invoked when this service is removed as a quorum member.
     */
    void memberRemove();

	/**
	 * Invoked when a consensus has been achieved among <code>(k+1)/2</code>
	 * services concerning a shared lastCommitTime.
	 * 
	 * @param lastCommitTime
	 *            The last commit time around which a consensus was established.
	 */
    void consensus(final long lastCommitTime);
    
    /**
     * Invoked when this service joins the quorum.
     */
    void serviceJoin();
    
    /**
     * Invoked when this service was joined with the quorum and it leaves the
     * quorum.
     */
    void serviceLeave();

    /**
     * Invoked for all quorum members when the leader leaves the quorum.
     * 
     * @param leaderId
     *            The {@link UUID} of the leader which left the quorum.
     */
    void leaderLeft(UUID leaderId);

    /**
     * Invoked for all quorum member when the quorum breaks.
     */
    void quorumBroke();

}
