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
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class GroupMemberValueExpressionNodeBase extends
        GroupMemberNodeBase implements IValueExpressionMetadata {

//    interface Annotations extends GroupMemberNodeBase.Annotations {
//        
//        String VALUE_EXPR = "valueExpr";
//        
//    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public GroupMemberValueExpressionNodeBase() {
    }

    public GroupMemberValueExpressionNodeBase(BOp[] args,
            Map<String, Object> anns) {

        super(args, anns);

    }

    public GroupMemberValueExpressionNodeBase(
            GroupMemberValueExpressionNodeBase op) {

        super(op);

    }

    abstract IValueExpressionNode getValueExpressionNode();

    abstract IValueExpression<? extends IV> getValueExpression();

    public Set<IVariable<?>> getConsumedVars() {

        final Set<IVariable<?>> consumedVars = new LinkedHashSet<IVariable<?>>();

        final Iterator<IVariable<?>> it = BOpUtility
                .getSpannedVariables(getValueExpression());

        while (it.hasNext()) {

            consumedVars.add(it.next());

        }

        return consumedVars;

    }

    public ComputedMaterializationRequirement getMaterializationRequirement() {

        return new ComputedMaterializationRequirement(getValueExpression());

    }

}
