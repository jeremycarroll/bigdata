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

package com.bigdata.relation.rule;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;

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

    public IBindingSet newBindingSet(IRule rule) {
        // TODO Auto-generated method stub
        return null;
    }

    public Object newElement(IPredicate predicate, IBindingSet bindingSet) {
        // TODO Auto-generated method stub
        return null;
    }

    public ISolution newSolution(IRule rule, IBindingSet bindingSet) {
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

    public long runMutation(ActionEnum action, IStep step) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public IChunkedOrderedIterator<ISolution> runQuery(IStep step) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    public IIndexManager getIndexManager() {
        // TODO Auto-generated method stub
        return null;
    }

    public long getReadTimestamp() {
        return ITx.READ_COMMITTED;
    }

    public long getWriteTimestamp() {
        return ITx.UNISOLATED;
    }

    public int solutionFlags() {
        return IJoinNexus.ELEMENT | IJoinNexus.BINDINGS | IJoinNexus.RULE;
    }

    public IRuleTaskFactory getRuleTaskFactory(boolean parallel, IRule rule) {
        // TODO Auto-generated method stub
        return null;
    }

    public IJoinNexusFactory getJoinNexusFactory() {
        // TODO Auto-generated method stub
        return null;
    }

}
