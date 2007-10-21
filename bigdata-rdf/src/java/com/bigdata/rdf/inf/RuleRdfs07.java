/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.rdf.inf;

import com.bigdata.rdf.spo.SPO;

/**
 * rdfs7:
 * <pre>
 *       triple(?u,?b,?y) :-
 *          triple(?a,rdfs:subPropertyOf,?b),
 *          triple(?u,?a,?y).
 * </pre>
 */
public class RuleRdfs07 extends AbstractRuleRdfs_2_3_7_9 {

    public RuleRdfs07( InferenceEngine store, Var a, Var b, Var u, Var y ) {

        super(store, new Triple(u, b, y),
                new Pred[] {
                new Triple(a, store.rdfsSubPropertyOf, b),
                new Triple(u, a, y)
                });

    }
    
    protected SPO buildStmt3( SPO stmt1, SPO stmt2 ) {

        return new SPO( stmt2.s, stmt1.o, stmt2.o );
        
    }

}
