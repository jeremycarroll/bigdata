/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 25, 2008
 */

package com.bigdata.join;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockJoinNexus implements IJoinNexus {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public MockJoinNexus() {
        // TODO Auto-generated constructor stub
    }

    public void copyValues(Object e, IPredicate predicate, IBindingSet bindingSet) {
        // TODO Auto-generated method stub
        
    }

    public IRelationLocator getRelationLocator() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isElementOnly() {
        // TODO Auto-generated method stub
        return false;
    }

    public IBindingSet newBindingSet(IRule rule) {
        // TODO Auto-generated method stub
        return null;
    }

    public Object newElement(IPredicate predicate, IBindingSet bindingSet) {
        // TODO Auto-generated method stub
        return null;
    }

    public RuleState newRuleState(IRule rule) {
        // TODO Auto-generated method stub
        return null;
    }

    public ISolution newSolution(IRule rule, IBindingSet bindingSet) {
        // TODO Auto-generated method stub
        return null;
    }

    public IEvaluationPlan newEvaluationPlan(IRule rule) {
        // TODO Auto-generated method stub
        return null;
    }

    public IBuffer<ISolution> newDeleteBuffer(IMutableRelation relation) {
        // TODO Auto-generated method stub
        return null;
    }

    public IBuffer<ISolution> newInsertBuffer(IMutableRelation relation) {
        // TODO Auto-generated method stub
        return null;
    }

    public IBlockingBuffer<ISolution> newQueryBuffer() {
        // TODO Auto-generated method stub
        return null;
    }

    public long runMutation(ActionEnum action, IProgram program) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public IChunkedOrderedIterator<ISolution> runQuery(IProgram program) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

}
