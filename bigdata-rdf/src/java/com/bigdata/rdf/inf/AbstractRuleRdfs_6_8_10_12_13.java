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

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

public class AbstractRuleRdfs_6_8_10_12_13 extends AbstractRuleRdf {

    public AbstractRuleRdfs_6_8_10_12_13
        ( InferenceEngine inf, 
          Triple head, 
          Triple body
          ) {

        super(inf, head, new Pred[] { body });

    }
    
    public RuleStats apply( final RuleStats stats, final SPOBuffer buffer) {
        
        final long computeStart = System.currentTimeMillis();
        
        final RdfKeyBuilder keyBuilder = db.getKeyBuilder();
        
        byte[] fromKey = keyBuilder.statement2Key
            ( body[0].p.id, body[0].o.id, NULL
              );
            
        byte[] toKey = keyBuilder.statement2Key
            ( body[0].p.id, body[0].o.id+1, NULL
              );
        
//        IEntryIterator it = db.getPOSIndex().rangeIterator(fromKey, toKey); 
//        
//        while ( it.hasNext() ) {
//            
//            it.next();
//            
//            stats.stmts1++;
//
//            SPO stmt = new SPO(KeyOrder.POS, keyBuilder, it.getKey());
//            
//            long _s = head.s.isVar() ? stmt.s : head.s.id;
//            long _p = head.p.isVar() ? stmt.s : head.p.id;
//            long _o = head.o.isVar() ? stmt.s : head.o.id;
//        
//            buffer.add( new SPO(_s, _p, _o) );
//
//            stats.numComputed++;
//            
//        }

        SPO[] stmts1 = db.getStatements(KeyOrder.POS, fromKey, toKey);
        
        stats.stmts1 += stmts1.length;
        
        for (SPO stmt1 : stmts1) {
            
            // @todo review -- should this be substituting stmt.s in each case?

            long s = head.s.isVar() ? stmt1.s : head.s.id;
            long p = head.p.isVar() ? stmt1.s : head.p.id;
            long o = head.o.isVar() ? stmt1.s : head.o.id;
      
            SPO newSPO = new SPO(s, p, o, StatementEnum.Inferred);
            
            Justification jst = null;
            
            if(justify) {
                
                jst = new Justification(this, newSPO, new SPO[] { stmt1 });
                
            }
            
            buffer.add( newSPO, jst );

        }

        stats.numComputed += stmts1.length;

        stats.elapsed += System.currentTimeMillis() - computeStart;
        
        return stats;
        
    }

}