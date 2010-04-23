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
 * Created on Oct 14, 2006
 */

package com.bigdata.journal.ha;

import java.util.Properties;

import junit.framework.TestCase;

import com.bigdata.journal.AbstractJournalTestCase;
import com.bigdata.journal.Journal;
import com.bigdata.journal.ProxyTestCase;

/**
 * <p>
 * Abstract harness for testing under a variety of configurations. In order to
 * test a specific configuration, create a concrete instance of this class. The
 * configuration can be described using a mixture of a <code>.properties</code>
 * file of the same name as the test class and custom code.
 * </p>
 * <p>
 * When debugging from an IDE, it is very helpful to be able to run a single
 * test case. You can do this, but you MUST define the property
 * <code>testClass</code> as the name test class that has the logic required
 * to instantiate and configure an appropriate object manager instance for the
 * test.
 * </p>
 */
abstract public class AbstractHAJournalTestCase
    extends AbstractJournalTestCase
{

    //
    // Constructors.
    //

    public AbstractHAJournalTestCase() {}
    
    public AbstractHAJournalTestCase(String name) {super(name);}

    //************************************************************
    //************************************************************
    //************************************************************
    
    /**
     * Invoked from {@link TestCase#setUp()} for each test in the suite.
     */
    public void setUp(ProxyTestCase testCase) throws Exception {

        super.setUp(testCase);
        
//        if(log.isInfoEnabled())
//        log.info("\n\n================:BEGIN:" + testCase.getName()
//                + ":BEGIN:====================");

    }

    /**
     * Invoked from {@link TestCase#tearDown()} for each test in the suite.
     */
    public void tearDown(ProxyTestCase testCase) throws Exception {

        super.tearDown(testCase);

        if (quorumManager != null)
            quorumManager.destroy();

    }

    private MockQuorumManager quorumManager;

    @Override
    protected Journal getStore(final Properties properties) {

        quorumManager = AbstractHAJournalTestCase.this
                .newQuorumManager(properties);

        // return the master.
        return quorumManager.stores[0];

    }

    protected MockQuorumManager newQuorumManager(final Properties properties) {

        return new MockQuorumManager(properties);

    };

    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is closed or if the store can not be
     *                re-opened, e.g., from failure to obtain a file lock, etc.
     */
    @Override
    protected Journal reopenStore(final Journal store) {

        assertTrue(store.getQuorumManager()==quorumManager);
        
//        final MockQuorumManager quorumManager = ((MockQuorumManager) store
//                .getQuorumManager());

        quorumManager.reopen();

        return quorumManager.stores[0];

    }

}
