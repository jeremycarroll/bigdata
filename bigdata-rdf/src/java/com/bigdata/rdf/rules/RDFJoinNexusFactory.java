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
 * Created on Jul 9, 2008
 */

package com.bigdata.rdf.rules;

import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IEvaluationPlan;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;

/**
 * Factory for {@link RDFJoinNexus} objects.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFJoinNexusFactory implements IJoinNexusFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 8270873764858640472L;
   
    final RuleContextEnum ruleContext;
    final ActionEnum action;
    final long writeTimestamp;
    final long readTimestamp;
    final boolean justify;
    /**
     * true for high level query and false for closure. when <code>true</code>,
     * query time inferences are included in an read on the access path using
     * {@link InferenceEngine#backchainIterator(IPredicate)}.
     */
    final boolean backchain;
    final boolean forceSerialExecution;
    final int bufferCapacity;
    final int solutionFlags;
    @SuppressWarnings("unchecked")
	final IElementFilter filter;
    final IEvaluationPlanFactory planFactory;

    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{ ruleContext="+ruleContext);

        sb.append(", action="+action);

        sb.append(", writeTime="+writeTimestamp);
        
        sb.append(", readTime="+readTimestamp);
        
        sb.append(", justify="+justify);
        
        sb.append(", backchain="+backchain);
        
        sb.append(", forceSerialExecution="+forceSerialExecution);
        
        sb.append(", bufferCapacity="+bufferCapacity);
        
        sb.append(", solutionFlags="+solutionFlags);
        
        sb.append(", filter="+(filter==null?"N/A":filter.getClass().getName()));

        sb.append(", planFactory="+planFactory.getClass().getName());

        sb.append("}");
        
        return sb.toString();
        
    }

	/**
     * 
     * @param action
     *            Indicates whether this is a Query, Insert, or Delete
     *            operation.
     * @param writeTimestamp
     *            The timestamp of the relation view(s) using to write on the
     *            {@link IMutableRelation}s (ignored if you are not execution
     *            mutation programs).
     * @param readTimestamp
     *            The timestamp of the relation view(s) used to read from the
     *            access paths.
     * @param forceSerialExecution
     *            When <code>true</code>, rule sets will be forced to execute
     *            sequentially even when they are not flagged as a sequential
     *            program.
     * @param justify
     *            if justifications are required.
     * @param bufferCapacity
     *            The capacity of the buffers used to support chunked iterators
     *            and efficient ordered writes.
     * @param solutionFlags
     *            Flags controlling the behavior of
     *            {@link #newSolution(IRule, IBindingSet)}.
     * @param filter
     *            An optional filter that will be applied to keep matching
     *            elements out of the {@link IBuffer} for Query or Mutation
     *            operations.
     * @param planFactory
     *            The factory used to generate {@link IEvaluationPlan}s for
     *            {@link IRule}s.
     */
	public RDFJoinNexusFactory(RuleContextEnum ruleContext, ActionEnum action,
            long writeTime, long readTime, boolean forceSerialExecution,
            boolean justify, int bufferCapacity, int solutionFlags,
            IElementFilter filter, IEvaluationPlanFactory planFactory) {

        if (ruleContext == null)
            throw new IllegalArgumentException();

        if (action == null)
            throw new IllegalArgumentException();

        if (planFactory == null)
            throw new IllegalArgumentException();

        this.ruleContext = ruleContext;

        this.action = action;
        
        this.writeTimestamp = writeTime;

        this.readTimestamp = readTime;

        this.justify = justify;

        this.backchain = ruleContext == RuleContextEnum.HighLevelQuery ? true
                : false;
        
        this.forceSerialExecution = forceSerialExecution;
        
        this.bufferCapacity = bufferCapacity;
        
        this.solutionFlags = solutionFlags;

        this.filter = filter;
        
        this.planFactory = planFactory;

    }

    public IJoinNexus newInstance(IIndexManager indexManager) {

        return new RDFJoinNexus(this, indexManager);

    }

}
