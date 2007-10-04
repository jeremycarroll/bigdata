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
 * Created on Mar 3, 2006
 */
package com.bigdata.concurrent.locking.action;

import com.bigdata.concurrent.TestConcurrencyControl.ResourceQueue;
import com.bigdata.concurrent.schedule.Action;
import com.bigdata.concurrent.schedule.Tx;

/**
 * Helper class defines an action used to lock a resource.
 * 
 * @author thompsonbry
 * 
 * @param R
 *            The type of the object that models a "resource".
 * 
 * @param T
 *            The type of the object that models a "transaction" or other kind
 *            of operation in a multi-programming system.
 */
public class LockAction<R,T extends Tx> extends Action<T> {

    final private ResourceQueue<R, T> queue;

    /**
     * Action requests a lock and MUST NOT block.
     * 
     * @param tx
     *            The transaction.
     * @param queue
     *            The queue for the resource for which the lock will be
     *            requested.
     */
    public LockAction(T tx, ResourceQueue<R,T> queue) {
        this(tx, queue, false);
    }

    /**
     * Action requests a lock and MUST NOT block.
     * 
     * @param tx
     *            The transaction.
     * @param queue
     *            The queue for the resource for which the lock will be
     *            requested.
     * @param blocks
     *            <code>true</code> iff the lock request will block.
     *            (This is verified during execution.)
     */
    public LockAction(T tx, ResourceQueue<R,T> queue, boolean blocks) {
        super(tx, "lock(tx=" + tx + ", resource=" + queue.getResource() + ", blockes="+blocks+")", blocks);
        this.queue = queue;
    }

    public void run() {
    	if( Thread.currentThread() != getTx() ) {
    		// verify that we are running the the correct thread.
    		throw new AssertionError();
    	}
        try {
            queue.lock(getTx());
        } catch (InterruptedException e) {
            // TODO Should terminate the operation.
            e.printStackTrace();
            throw new RuntimeException("Interrupted: "+e,e);
        }
    }
}
