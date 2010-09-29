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

package com.bigdata.journal;

import java.util.Collection;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;

/**
 * <p>
 * This class provides proxy delegation logic for abstract methods declared by
 * {@link AbstractJournalTestCase} and is used to extend the set of tests that will be
 * applied to all implementations of the generic object model Java API. If you
 * want to test a new implementation, you MUST extend the
 * {@link AbstractJournalTestCase} instead and implement its abstract methods for your
 * implementation. This class provides an implementation neutral way to add new
 * tests, not a means for testing specific generic object model Java API
 * implementations.
 * </p>
 * <p>
 * In order to add new tests for the generic object model Java APIs, you extend
 * this class and write test methods.
 * </p>
 * 
 * @see AbstractJournalTestCase
 */
public abstract class ProxyTestCase<S extends IIndexManager>
    extends AbstractIndexManagerTestCase<S> {

    public ProxyTestCase() {}

    public static Collection<Object[]> getDelegateGroup1() {
        Object[][] list = new Object[][] {
            { new com.bigdata.journal.TestTransientJournal() },
            //{ new com.bigdata.journal.TestDirectJournal()    } ,
            { new com.bigdata.journal.TestWORMStrategy()     } };
        return java.util.Arrays.asList(list);
    };

    public static Collection<Object[]> getDelegateGroup2() {
        Object[][] list = new Object[][] {
            { new com.bigdata.service.TestJournal()   },
            { new com.bigdata.service.TestEDSRemote() },
            { new com.bigdata.service.TestEDS()       } } ;
        return java.util.Arrays.asList(list);
    };

    //************************************************************
    //************************ IProxyTest ************************
    //************************************************************

    private Object m_delegate = null;

    public void setDelegate(Object delegate) {

        m_delegate = delegate;

    }

    public Object getDelegate() throws IllegalStateException {

        return m_delegate;

    }

    /**
     * Returns the delegate after first making sure that it is non-null and
     * extends {@link AbstractJournalTestCase}.
     */

    public AbstractIndexManagerTestCase<S> getOurDelegate() {

        if (m_delegate == null) {

            /*
             * This property gives the class name of the concrete instance of
             * AbstractTestSuite that we need to instantiate so that we can run
             * or debug a single test at a time! This is designed to support
             * running or debugging a single test that has failed after running
             * the entire test suite in an IDE such as Eclipse.
             * 
             * Note: We reach out to System.getProperty() and not
             * getProperties() to avoid infinite recursion through
             * getOurDelegate. The only way this makes sense anyway is when you
             * define -DtestClass=... as a JVM property.
             * 
             * @todo document.
             */
            String testClass = System.getProperty("testClass");
            if (testClass == null) {

                throw new IllegalStateException(
                        "testClass: property not defined, could not configure delegate.");

            }
            try {
                Class cl = Class.forName(testClass);
                m_delegate = cl.newInstance();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            // throw new IllegalStateException
            // ( "The delegate is not configured."
            // );

        }

        if (m_delegate instanceof AbstractIndexManagerTestCase) {

            return (AbstractIndexManagerTestCase) m_delegate;

        }

        throw new IllegalStateException("The delegate MUST extend "
                + AbstractIndexManagerTestCase.class.getName() + ", not "
                + m_delegate.getClass().getName());

    }

    //************************************************************
    //********************* proxied methods **********************
    //************************************************************

    /*
     * Note: All methods on the delegate MUST be proxied here or they will be
     * answered by our base class which is the same Class as the delegate, but
     * whose instance fields have not been initialized! (An instance of the
     * proxy is created for each test, while one instance of the delegate serves
     * an entire suite of tests.)
     */
    
    @Before
    public void setUp() throws Exception {
        getOurDelegate().setUp(this);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        getOurDelegate().tearDown(this);
    }

    @Override
    public Properties getProperties() {
        return getOurDelegate().getProperties();
    }

    /**
     * Open/create an {@link IIndexManager} using the properties reported by
     * {@link #getProperties()}.
     */
    public S getStore() {
        return getOurDelegate().getStore(getProperties());
    }

    /**
     * Open/create an {@link IIndexManager} using the given properties.
     */
    public S getStore(Properties properties) {
        return getOurDelegate().getStore(properties);
    }

    /**
     * Close the {@link IIndexManager} and re-open an {@link IIndexManager}
     * backed by the same persistent storage.
     */
    public S reopenStore(S store) {
        return getOurDelegate().reopenStore(store);
    }

}
