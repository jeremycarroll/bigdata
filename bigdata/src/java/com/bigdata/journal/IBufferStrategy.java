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
package com.bigdata.journal;

import java.io.IOException;
import java.io.RandomAccessFile;

import com.bigdata.rawstore.IMRMW;
import com.bigdata.rawstore.IRawStore;

/**
 * <p>
 * Interface for implementations of a buffer strategy as identified by a
 * {@link BufferMode}. This interface is designed to encapsulate the
 * specifics of reading and writing slots and performing operations to make
 * an atomic commit.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBufferStrategy extends IRawStore, IMRMW {
    
    /**
     * The next offset at which a data item would be written on the store.
     */
    public long getNextOffset();
    
    /**
     * The buffer mode supported by the implementation
     * 
     * @return The implemented buffer mode.
     */
    public BufferMode getBufferMode();
    
    /**
     * The initial extent.
     */
    public long getInitialExtent();
    
    /**
     * The maximum extent allowable before a buffer overflow operation will be
     * rejected.
     * <p>
     * Note: The semantics here differ from those defined by
     * {@link Options#MAXIMUM_EXTENT}. The latter specifies the threshold at
     * which a journal will overflow (onto another journal) while this specifies
     * the maximum size to which a buffer is allowed to grow.
     * <p>
     * Note: This is <em>normally</em> zero (0L), which basically means that
     * the maximum extent is ignored by the {@link IBufferStrategy} but
     * respected by the {@link AbstractJournal}, resulting in a <i>soft limit</i>
     * on journal overflow. The primary reason to limit the buffer size is when
     * an in-memory buffer will be converted to a disk-based buffer -- see
     * {@link TemporaryRawStore} for an example.
     * 
     * @return The maximum extent permitted for the buffer -or- <code>0L</code>
     *         iff no limit is imposed.
     */
    public long getMaximumExtent();
    
    /**
     * The current size of the journal in bytes.  When the journal is backed by
     * a disk file this is the actual size on disk of that file.  The initial
     * value for this property is set by {@link Options#INITIAL_EXTENT}.
     */
    public long getExtent();

    /**
     * The size of the user data extent in bytes.
     * <p>
     * Note: The size of the user extent is always generally smaller than the
     * value reported by {@link #getExtent()} since the latter also reports the
     * space allocated to the journal header and root blocks.
     */
    public long getUserExtent();
    
    /**
     * Either truncates or extends the journal.
     * <p>
     * Note: Implementations of this method MUST be synchronized so that the
     * operation is atomic with respect to concurrent writers.
     * 
     * @param extent
     *            The new extent.
     * 
     * @exception IllegalArgumentException
     *                The user extent MAY NOT be increased beyond the maximum
     *                offset for which the journal was provisioned by
     *                {@link Options#OFFSET_BITS}.
     */
    public void truncate(long extent);

    /**
     * Write the root block onto stable storage (ie, flush it through to disk).
     * 
     * @param rootBlock
     *            The root block. Which root block is indicated by
     *            {@link IRootBlockView#isRootBlock0()}.
     * 
     * @param forceOnCommit
     *            Governs whether or not the journal is forced to stable storage
     *            and whether or not the file metadata for the journal is forced
     *            to stable storage. See {@link Options#FORCE_ON_COMMIT}.
     */
    public void writeRootBlock(IRootBlockView rootBlock,
            ForceEnum forceOnCommitEnum);
    
    /**
     * A block operation that transfers the serialized records (aka the written
     * on portion of the user extent) en mass from the buffer onto an output
     * file. The buffered records are written "in order" starting at the current
     * position on the output file. The file is grown if necessary.
     * <p>
     * Note: Implementations of this method MUST be synchronized so that the
     * operation is atomic with respect to concurrent writers.
     * 
     * @param out
     *            The file to which the buffer contents will be transferred.
     * 
     * @return The #of bytes written.
     * 
     * @throws IOException
     */
    public long transferTo(RandomAccessFile out) throws IOException;

//    /**
//     * Seals the store against further writes and discards any write caches
//     * since they will no longer be used.  The method should be implemented
//     * such that concurrent readers are not disturbed.
//     */
//    public void closeForWrites();
    
}
