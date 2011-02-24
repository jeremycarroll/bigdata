/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Feb 23, 2011
 */

package com.bigdata.bop.joinGraph.rto;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rwstore.sector.IMemoryManager;

/**
 * Base class for a sample taken from a vertex (access path) or edge (cutoff
 * join).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class SampleBase {

    /**
     * The estimated cardinality of the underlying access path (for a vertex) or
     * the join (for a cutoff join).
     */
    public final long estimatedCardinality;

    /**
     * The limit used to produce the {@link #sample}.
     */
    public final int limit;

    /**
     * Indicates whether the estimate is exact, an upper bound, or a lower
     * bound.
     * 
     * TODO When the input to a cutoff join is {@link EstimateEnum#Exact}, we
     * could run the join against the sample rather than the disk by wrapping
     * the sample as an inline access path.
     * 
     * TODO This field should be used to avoid needless re-computation of a join
     * whose exact solution is already known. We already do this within the
     * runtime optimizer. To go further than that we need to do the partial
     * evaluation of the join graph.
     */
    public final EstimateEnum estimateEnum;

    /**
     * Return <code>true</code> iff this sample is the fully materialized
     * solution for the vertex or join path segment.
     */
    public boolean isExact() {

        return estimateEnum == EstimateEnum.Exact;

    }

    /**
     * Sample.
     * 
     * TODO Large samples should be buffered on the {@link IMemoryManager} so
     * they do not pose a burden on the heap. This will require us to manage the
     * allocation contexts so we can release samples in a timely manner once
     * they are no longer used and always release samples by the time the RTO is
     * finished. [There is an additional twist if we have fully materialized
     * some part of the join since we no longer need to evaluate that path
     * segment.  If the RTO can interleave query evaluation with exploration
     * then we can take advantage of these materialized solutions.]
     */
    final IBindingSet[] sample;

    /**
     * 
     * @param estimatedCardinality
     *            The estimated cardinality.
     * @param limit
     *            The cutoff limit used to make that cardinality estimate.
     * @param estimateEnum
     *            Type safe enumeration indication various edge conditions which
     *            can arise when making a cardinality estimate.
     * @param sample
     *            The sample.
     */
    public SampleBase(//
            final long estimatedCardinality,//
            final int limit,//
            final EstimateEnum estimateEnum,//
            final IBindingSet[] sample//
            ) {

        if (estimatedCardinality < 0L)
            throw new IllegalArgumentException();

        if (limit <= 0)
            throw new IllegalArgumentException();

        if (estimateEnum == null)
            throw new IllegalArgumentException();

        if (sample == null)
            throw new IllegalArgumentException();

        this.estimatedCardinality = estimatedCardinality;

        this.limit = limit;

        this.estimateEnum = estimateEnum;

        this.sample = sample;

    }

    /**
     * Hook for extending {@link #toString()}.
     * 
     * @param sb
     *            The buffer into which the implementation can write additional
     *            information.
     */
    protected void toString(final StringBuilder sb) {
        // NOP
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("{estimatedCardinality=" + estimatedCardinality);
        sb.append(",limit=" + limit);
        sb.append(",estimateEnum=" + estimateEnum);
        sb.append(",sampleSize=" + sample.length);
        toString(sb); // allow extension
        sb.append("}");
        return sb.toString();
    }

}
