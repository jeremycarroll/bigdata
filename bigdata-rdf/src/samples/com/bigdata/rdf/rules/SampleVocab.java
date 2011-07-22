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
package com.bigdata.rdf.rules;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.RDFSVocabulary;

public class SampleVocab extends RDFSVocabulary {

	private static final long serialVersionUID = -6646094201371729113L;
	
    /**
     * De-serialization ctor.
     */
    public SampleVocab() {
        
        super();
        
    }
    
    /**
     * Used by {@link AbstractTripleStore#create()}.
     * 
     * @param db
     *            The database.
     */
    public SampleVocab(AbstractTripleStore db) {

        super( db );
        
    }

    /**
     * Add any values used by custom inference rules.
     */
    @Override
    protected void addValues() {

    	super.addValues();
    	
        add(SAMPLE.SIMILAR_TO);
        
    }

}
