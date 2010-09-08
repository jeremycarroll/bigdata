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

package junit.extensions.proxy;

import junit.framework.*;

/**
 * A {@link Test} that holds a reference to a delegate.  Normally the
 * delegate will extend {@link TestCase} to provide, possibly
 * persistent, implementation specific logic for establishing test
 * fixtures.  The delegate is normally an instance of your concrete
 * implementation specific test harness with access to any required
 * configuration data, e.g., a properties file.
 */

public interface IProxyTest
    extends Test
{

    /**
     * Sets the delegate.  {@link ProxyTestSuite} uses this method to
     * set the delegate on each test class instance that it creates
     * that implements the {@link IProxyTest} interface.
     */

    public void setDelegate( Test delegate );

    /**
     * Returns the reference to the delegate or <code>null</code> if
     * the delegate was not established.
     */

    public Test getDelegate();

}
