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
package com.bigdata.relation.rdf.rules;

import com.bigdata.relation.IRelationName;
import com.bigdata.relation.rdf.SPO;
import com.bigdata.relation.rdf.SPOPredicate;
import com.bigdata.relation.rule.Rule;

/**
 * rdfs8:
 * 
 * <pre>
 *  (?u rdfs:subClassOf rdfs:Resource) :-
 *     (?u rdf:type rdfs:Class).
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleRdfs08 extends Rule {

    /**
     * 
     */
    private static final long serialVersionUID = -5143563937919466754L;

    public RuleRdfs08(IRelationName<SPO> relationName, RDFSVocabulary inf) {

        super(  "rdfs08",//
                new SPOPredicate(relationName,var("u"), inf.rdfsSubClassOf, inf.rdfsResource),//
                new SPOPredicate[]{//
                    new SPOPredicate(relationName,var("u"), inf.rdfType, inf.rdfsClass)//
                }, //
                null // constraints
                );

    }
    
}