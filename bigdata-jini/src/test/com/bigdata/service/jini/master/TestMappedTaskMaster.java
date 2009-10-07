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
 * Created on Oct 7, 2009
 */

package com.bigdata.service.jini.master;

import junit.framework.TestCase2;

import com.bigdata.service.jini.util.JiniServicesHelper;

/**
 * Unit tests for the {@link MappedTaskMaster}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMappedTaskMaster extends TestCase2 {

    /**
     * 
     */
    public TestMappedTaskMaster() {
    }

    /**
     * @param name
     */
    public TestMappedTaskMaster(String name) {
        super(name);
    }
    
    private JiniServicesHelper helper;
    
    protected void setUp() throws Exception {
        
        super.setUp();

        helper = new JiniServicesHelper();

    }

    protected void tearDown() throws Exception {

        if (helper != null) {

            helper.destroy();
            
        }
        
        super.tearDown();
        
    }
    
    public void test() {

        /*
         * FIXME Finish this unit test. The problem is that we need to specify
         * the RDFDataLoadMaster component in the configuration. This could be
         * done using overrides, but there has to be a more pleasant way to go.
         * Concatenating onto the end of the configuration file is another
         * route.
         */
        fail("finish test");
//        new RDFDataLoadMaster<JobState, Callable<U>, U>(helper.getFederation());
        
    }
    
}
