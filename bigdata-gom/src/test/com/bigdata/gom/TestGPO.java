/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
package com.bigdata.gom;

import java.net.URL;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFFormat;

import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.ILinkSet;

public class TestGPO extends LocalGOMTestCase {

	/**
	 * The initial state rdf store is defined in the testgom.n3 file
	 */
	protected void doLoadData() {
		final URL n3 = TestGOM.class.getResource("testgom.n3");

		try {
			load(n3, RDFFormat.N3);
		} catch (Exception e) {
			fail("Unable to load test data");
		}
	}

	public void testLinkSetsIn() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI clssuri = vf.createURI("gpo:#1");
	    IGPO clssgpo = om.getGPO(clssuri);
		
	    ILinkSet ls = clssgpo.getLinksIn(vf.createURI("attr:/type"));
	    
	    checkLinkSet(ls, 2);
	}

	/**
	 * Checks consistency with added and removed values
	 */
	public void testLinkSetsOut() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
		
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    ILinkSet ls = workergpo.getLinksOut(worksFor);
	    
	    checkLinkSet(ls, 2);
	    
	    final URI gpo678uri = vf.createURI("gpo:#678");
	    workergpo.addValue(worksFor, gpo678uri);
	    
	    checkLinkSet(ls, 3);
	    
	    workergpo.removeValue(worksFor, gpo678uri);

	    checkLinkSet(ls, 2);
	    
	    workergpo.removeValues(worksFor);
	    
	    checkLinkSet(ls, 0);
	}
	
	/**
	 * Checks linkSet membership
	 */
	public void testMembership() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
		
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    ILinkSet ls = workergpo.getLinksOut(worksFor);
	    
	    checkLinkSet(ls, 2);

	    final URI companyuri = vf.createURI("gpo:#456");
	    IGPO companygpo = om.getGPO(companyuri);
	    
	    assertTrue(companygpo.isMemberOf(ls));

	}

}
