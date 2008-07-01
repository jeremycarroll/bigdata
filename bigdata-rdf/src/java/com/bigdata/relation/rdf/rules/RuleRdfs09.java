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
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.NE;
import com.bigdata.relation.rule.Rule;

/**
 * rdfs9:
 * <pre>
 *       triple(?v,rdf:type,?x) :-
 *          triple(?u,rdfs:subClassOf,?x),
 *          triple(?v,rdf:type,?u). 
 * </pre>
 */
public class RuleRdfs09 extends Rule {

    /**
     * 
     */
    private static final long serialVersionUID = 6301379050758674236L;

    public RuleRdfs09( IRelationName<SPO> relationName, RDFSVocabulary inf) {

        super( "rdfs09", new SPOPredicate(relationName,var("v"), inf.rdfType, var("x")),//
                new SPOPredicate[] {//
                    new SPOPredicate(relationName,var("u"), inf.rdfsSubClassOf, var("x")),//
                    new SPOPredicate(relationName,var("v"), inf.rdfType, var("u"))//
                },
                new IConstraint[] {
                    new NE(var("u"),var("x"))
                });
    }

}
