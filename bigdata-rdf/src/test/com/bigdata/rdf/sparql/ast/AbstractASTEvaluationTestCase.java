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
 * Created on Aug 29, 2011
 */
/* Portions of this file are:
 * 
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2008.
 *
 * Licensed under the Aduna BSD-style license.
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractASTEvaluationTestCase extends AbstractQueryEngineTestCase {

    private static final Logger log = Logger
            .getLogger(AbstractASTEvaluationTestCase.class);

    /**
     * 
     */
    public AbstractASTEvaluationTestCase() {
    }

    public AbstractASTEvaluationTestCase(String name) {
        super(name);
    } 

    protected AbstractTripleStore store = null;

    protected String baseURI = null;

    protected void setUp() throws Exception {
        
        super.setUp();
        
        store = getStore(getProperties());
        
        /*
         * Note: This needs to be an absolute URI.
         */
        
        baseURI = "http://www.bigdata.com";
        
    }
    
    protected void tearDown() throws Exception {
        
        if (store != null) {
        
            store.__tearDownUnitTest();
            
            store = null;
        
        }
        
        baseURI = null;
        
        super.tearDown();
        
    }

    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        // turn on quads.
        properties.setProperty(AbstractTripleStore.Options.QUADS, "true");

//        // override the default vocabulary.
//        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
//                NoVocabulary.class.getName());

        // turn off axioms.
        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        // no persistence.
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());
        
        return properties;

    }

    protected AbstractTripleStore getStore(final Properties properties) {

        final String namespace = "kb";

        // create/re-open journal.
        final Journal journal = new Journal(properties);

        final LocalTripleStore lts = new LocalTripleStore(journal, namespace,
                ITx.UNISOLATED, properties);

        lts.create();

        return lts;

    }

    static protected void assertSameAST(final IQueryNode expected,
            final IQueryNode actual) {

        if (!expected.equals(actual)) {

            log.error("expected: " + BOpUtility.toString((BOp) expected));
            log.error("actual  : " + BOpUtility.toString((BOp) actual));

            AbstractQueryEngineTestCase.diff((BOp) expected, (BOp) actual);

            // No difference was detected?
            throw new AssertionError();
//            fail("expected:\n" + BOpUtility.toString((BOp) expected)
//                    + "\nactual:\n" + BOpUtility.toString((BOp) actual));

        }

    }

}
