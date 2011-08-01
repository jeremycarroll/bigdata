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
package com.bigdata.bop.rdf.aggregate;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;

/**
 * Operator reports an arbitrary value from presented binding sets for the given
 * variable. Missing values are not chosen when non-missing values are
 * available.
 * 
 * @author thompsonbry
 */
public class SAMPLE extends AggregateBase<IV> implements IAggregate<IV> {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    public SAMPLE(BOpBase op) {
        super(op);
    }

    public SAMPLE(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    public SAMPLE(boolean distinct, IValueExpression<IV> expr) {
        super(FunctionCode.SAMPLE, distinct, expr);
    }

    /**
     * The sampled value and initially <code>null</code>.
     * <p>
     * Note: This field is guarded by the monitor on the {@link SAMPLE}
     * instance.
     */
    private transient IV sample = null;

    synchronized public IV get(final IBindingSet bindingSet) {

        final IValueExpression<IV<?, ?>> expr = (IValueExpression<IV<?, ?>>) get(0);

        final IV<?, ?> iv = expr.get(bindingSet);

        if (iv != null && sample == null) {

            // Take the first non-null observed value.
            sample = iv;

        }

        return sample;

    }

    synchronized public void reset() {
        sample = null;
    }

    synchronized public IV done() {
        return sample;
    }

    /**
     * We can take a sample without materializing anything.
     */
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.NEVER;

    }

}
