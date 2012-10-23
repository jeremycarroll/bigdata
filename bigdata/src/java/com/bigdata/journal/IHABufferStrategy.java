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
 * Created on May 18, 2010
 */

package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.quorum.Quorum;

/**
 * A highly available {@link IBufferStrategy}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IHABufferStrategy extends IBufferStrategy {

    /**
     * Write a buffer containing data replicated from the master onto the local
     * persistence store.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    void writeRawBuffer(IHAWriteMessage msg, IBufferAccess b) throws IOException,
            InterruptedException;

    /**
     * Send an {@link IHAWriteMessage} and the associated raw buffer through the
     * write pipeline.
     * 
     * @param msg
     *            The {@link IHAWriteMessage}.
     * @param b
     *            The raw buffer. Bytes from position to limit will be sent.
     *            remaining() must equal {@link IHAWriteMessage#getSize()}.
     * 
     * @return The {@link Future} for that request.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    Future<Void> sendHALogBuffer(IHALogRequest req, IHAWriteMessage msg,
            IBufferAccess b) throws IOException, InterruptedException;

    /**
     * Read from the local store in support of failover reads on nodes in a
     * highly available {@link Quorum}.
     * 
     * @throws InterruptedException
     */
    ByteBuffer readFromLocalStore(final long addr) throws InterruptedException;

    /**
     * Extend local store for a highly available {@link Quorum}.
     * 
     * @throws InterruptedException
     */
    void setExtentForLocalStore(final long extent) throws IOException,
    		InterruptedException;
    
    /**
     * Called from {@link AbstractJournal} commit2Phase to ensure is able to
     * read committed data that has been streamed directly to the backing store.
     */
    void resetFromHARootBlock(final IRootBlockView rootBlock);

    /**
     * Return the #of {@link WriteCache} blocks that have been written out as
     * part of the current write set. This value is origin ZERO (0) and is reset
     * to ZERO (0) after each commit or abort.
     */
    long getBlockSequence();

}
