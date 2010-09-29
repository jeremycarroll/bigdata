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
 * Created on Sep 4, 2008
 */

package com.bigdata.rdf.sail;

import java.util.Properties;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.relation.AbstractResource;

/**
 * Test suite for the {@link BigdataSail} with quads enabled. The provenance
 * mode is disabled. Inference is disabled. This version of the test suite uses
 * the pipeline join algorithm.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestBigdataSailWithQuadsAndPipelineJoinsWithoutInlining extends AbstractBigdataSailTestCase {

    /**
     * 
     */
    public TestBigdataSailWithQuadsAndPipelineJoinsWithoutInlining() {
    }
    
    @Override
    protected BigdataSail getSail(final Properties properties) {
        
        return new BigdataSail(properties);
        
    }

    @Override
    public Properties getProperties() {

        final Properties properties = new Properties(super.getProperties());
/*
        properties.setProperty(Options.STATEMENT_IDENTIFIERS, "false");

        properties.setProperty(Options.QUADS, "true");

        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
*/
        properties.setProperty(Options.QUADS_MODE, "true");
        
        properties.setProperty(Options.TRUTH_MAINTENANCE, "false");

        properties.setProperty(AbstractResource.Options.NESTED_SUBQUERY, "false");

        properties.setProperty(Options.INLINE_LITERALS, "false");

        return properties;
        
    }
    
    @Override
    protected BigdataSail reopenSail(BigdataSail sail) {

        final Properties properties = sail.database.getProperties();

        if (sail.isOpen()) {

            try {

                sail.shutDown();

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }
        
        return getSail(properties);
        
    }

}
