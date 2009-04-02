/*

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
package com.bigdata.service;

import com.bigdata.btree.IndexSegment;
import com.bigdata.mdi.IPartitionMetadata;

/**
 * Describes a "split" of keys for a batch operation that are spanned by the
 * same index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Split {

    /**
     * The index partition that spans the keys in this split.
     */
    public final IPartitionMetadata pmd;

    /**
     * Index of the first key in this split.
     */
    public final int fromIndex;

    /**
     * Index of the first key NOT included in this split.
     */
    public final int toIndex;

    /**
     * The #of keys in this split (toIndex - fromIndex).
     */
    public final int ntuples;

    /**
     * Create a representation of a split point without specifying the from/to
     * tuple index.
     * 
     * @param pmd
     *            The metadata for the index partition within which the keys in
     *            this split lie.
     */
    public Split(final IPartitionMetadata pmd) {

        this(pmd, 0/* fromIndexIgnored */, 0/* toIndexIgnored */);

    }
    
    /**
     * Create a representation of a split point.
     * 
     * @param pmd
     *            The metadata for the index partition within which the keys in
     *            this split lie.
     * @param fromIndex
     *            The index of the first key that will enter that index
     *            partition (inclusive lower bound).
     * @param toIndex
     *            The index of the first key that will NOT enter that index
     *            partition (exclusive upper bound).
     * 
     * @todo The fromKey, toKey in the {@link IPartitionMetadata} fully specify
     *       the split so the fromIndex,toIndex here are really redundent.
     *       Perhaps the help when building an {@link IndexSegment} from the
     *       {@link Split}?
     */
    public Split(final IPartitionMetadata pmd, final int fromIndex, final int toIndex) {

//        assert pmd != null;

        this.pmd = pmd;

        if (fromIndex < 0)
            throw new IllegalArgumentException("fromIndex=" + fromIndex);

        if (toIndex < fromIndex)
            throw new IllegalArgumentException("fromIndex=" + fromIndex
                    + ", toIndex=" + toIndex);

        this.fromIndex = fromIndex;

        this.toIndex = toIndex;

        this.ntuples = toIndex - fromIndex;

    }

    /**
     * Hash code is based on the {@link IPartitionMetadata} hash code.
     */
    public int hashCode() {

        return pmd.hashCode();

    }

    public boolean equals(Split o) {

        if (fromIndex != o.fromIndex)
            return false;

        if (toIndex != o.toIndex)
            return false;

        if (ntuples != o.ntuples)
            return false;

        if (!pmd.equals(o.pmd))
            return false;

        return true;

    }

    /**
     * Human friendly representation.
     */
    public String toString() {
        
        return "Split"+
        "{ ntuples="+ntuples+
        ", fromIndex="+fromIndex+
        ", toIndex="+toIndex+
        ", pmd="+pmd+
        "}";
        
    }
    
}
