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
/*
 * Created on Jul 2, 2008
 */

package com.bigdata.rdf.rules;

import java.util.Set;

import com.bigdata.rdf.spo.SPO;
import com.bigdata.relation.IRelationIdentifier;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.IStepTask;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractRuleFastClosure_5_6_7_9 extends
        AbstractRuleFastClosure_3_5_6_7_9 {

    /**
     * @param name
     * @param database
     * @param rdfsSubPropertyOf
     * @param propertyId
     */
    public AbstractRuleFastClosure_5_6_7_9(
            String name,
            final IRelationIdentifier<SPO> database,
            final IRelationIdentifier<SPO> focusStore,
            final IConstant<Long> rdfsSubPropertyOf,
            final IConstant<Long> propertyId) {

        super(name, database, rdfsSubPropertyOf, propertyId,
        /*
         * Custom rule executor factory.
         */
        new IRuleTaskFactory() {

            public IStepTask newTask(IRule rule, IJoinNexus joinNexus,
                    IBuffer<ISolution> buffer) {

                return new FastClosureRuleTask(database, focusStore, rule,
                        joinNexus, buffer, /* P, */
                        rdfsSubPropertyOf, propertyId) {

                    public Set<Long> getSet() {

                        return getSubPropertiesOf(propertyId);

                    }

                };

            }

        });

    }

}
