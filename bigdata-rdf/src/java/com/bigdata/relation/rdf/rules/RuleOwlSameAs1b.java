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
/*
 * Created on Nov 1, 2007
 */

package com.bigdata.relation.rdf.rules;

import com.bigdata.relation.IRelationName;
import com.bigdata.relation.rdf.SPO;
import com.bigdata.relation.rdf.SPOPredicate;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.NE;
import com.bigdata.relation.rule.Rule;

/**
 * owl:sameAs1b
 * 
 * <pre>
 * (x owl:sameAs y), (y owl:sameAs z) -&gt; (x owl:sameAs z)
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleOwlSameAs1b extends Rule
{

    /**
     * 
     */
    private static final long serialVersionUID = -9190200251803516786L;

    /**
     * @param inf
     */
    public RuleOwlSameAs1b(IRelationName<SPO> relationName, RDFSVocabulary inf) {

        super(  "owlSameAs1b",//
                new SPOPredicate(relationName,var("x"), inf.owlSameAs, var("z")), //
                new SPOPredicate[] { //
                    new SPOPredicate(relationName,var("x"), inf.owlSameAs, var("y")),//
                    new SPOPredicate(relationName,var("y"), inf.owlSameAs, var("z"))//
                }, new IConstraint[] {
                    new NE(var("x"),var("y")),
                    new NE(var("y"),var("z")),
                    new NE(var("x"),var("z")),
                    }
                );
        
    }

}
