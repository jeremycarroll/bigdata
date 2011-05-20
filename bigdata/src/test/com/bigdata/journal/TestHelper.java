/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on May 19, 2011
 */

package com.bigdata.journal;

import junit.extensions.proxy.IProxyTest;
import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * Some helper methods for CI.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHelper {

    /**
     * Verify that any journal created by the test have been destroyed.
     * <p>
     * Note: This clears the counter as a side effect to prevent a cascade
     * of tests from being failed.
     */
    public static void checkJournalsClosed(final TestCase test) {

        checkJournalsClosed(test, null/*delegate*/);
        
    }

    /**
     * Verify that any journal created by the test have been destroyed (variant
     * when using an {@link IProxyTest}).
     * <p>
     * Note: This clears the counter as a side effect to prevent a cascade of
     * tests from being failed.
     * 
     * @param test
     *            The unit test instance.
     * @param testClass
     *            The instance of the delegate test class for a proxy test
     *            suite.  For example, TestWORMStrategy.
     */
    public static void checkJournalsClosed(final TestCase test,
            final TestCase testClass) {

        final int nopen = AbstractJournal.nopen.getAndSet(0);
        final int nclose = AbstractJournal.nclose.getAndSet(0);
        final int ndestroy = AbstractJournal.ndestroy.getAndSet(0);

        if (nopen != nclose) {

            /*
             * At least one journal was opened which was never closed.
             */

            Assert.fail("Test did not close journal(s)"//
                    + ": nopen=" + nopen //
                    + ", nclose=" + nclose//
                    + ", ndestroy=" + ndestroy //
                    + ", test=" + test.getClass() + "." + test.getName()//
                    + (testClass == null ? "" : ", testClass="
                            + testClass.getClass().getName())//
            );

        }

        if (nopen > 0 && ndestroy == 0) {

            /*
             * At least one journal was opened which was never explicitly
             * destroyed.
             */

            Assert.fail("Test did not destroy journal(s)"//
                    + ": nopen=" + nopen //
                    + ", nclose=" + nclose//
                    + ", ndestroy=" + ndestroy //
                    + ", test=" + test.getClass() + "." + test.getName()//
                    + (testClass == null ? "" : ", testClass="
                            + testClass.getClass().getName())//

            );

        }

    }

}
