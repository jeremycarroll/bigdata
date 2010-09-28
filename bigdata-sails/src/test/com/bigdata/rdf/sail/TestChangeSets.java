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
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.changesets.ChangeRecord;
import com.bigdata.rdf.sail.changesets.IChangeLog;
import com.bigdata.rdf.sail.changesets.IChangeRecord;
import com.bigdata.rdf.sail.changesets.IChangeRecord.ChangeAction;
import com.bigdata.rdf.store.BD;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestChangeSets extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestChangeSets.class);
    
    /**
     * 
     */
    public TestChangeSets() {
    }

    /**
     * @param arg0
     */
    public TestChangeSets(String arg0) {
        super(arg0);
    }

    public void testChangeSets() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        final TestChangeLog changeLog = new TestChangeLog();
        cxn.setChangeLog(changeLog);
        
        try {
    
            final BigdataValueFactory vf = (BigdataValueFactory) sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            final URI a = vf.createURI(ns+"A");
            final URI b = vf.createURI(ns+"B");
            final URI c = vf.createURI(ns+"C");
            
            final BigdataStatement[] explicit = new BigdataStatement[] {
                vf.createStatement(a, RDFS.SUBCLASSOF, b),
                vf.createStatement(b, RDFS.SUBCLASSOF, c),
            };

            final BigdataStatement[] inferred = new BigdataStatement[] {
                vf.createStatement(a, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(a, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(a, RDFS.SUBCLASSOF, a),
                vf.createStatement(a, RDFS.SUBCLASSOF, c),
                vf.createStatement(b, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(b, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(b, RDFS.SUBCLASSOF, b),
                vf.createStatement(c, RDF.TYPE, RDFS.CLASS),
                vf.createStatement(c, RDFS.SUBCLASSOF, RDFS.RESOURCE),
                vf.createStatement(c, RDFS.SUBCLASSOF, c),
            };
 
/**/
            cxn.setNamespace("ns", ns);

            for (BigdataStatement stmt : explicit) {
                cxn.add(stmt);
            }

            cxn.commit();//
            
            if (log.isDebugEnabled()) {
                log.debug("\n" + sail.getDatabase().dumpStore(true, true, false));
            }
            
            final Collection<IChangeRecord> expected = 
                new LinkedList<IChangeRecord>();
            for (BigdataStatement stmt : explicit) {
                expected.add(new ChangeRecord(stmt, ChangeAction.ADDED));
            }
            for (BigdataStatement stmt : inferred) {
                expected.add(new ChangeRecord(stmt, ChangeAction.ADDED));
            }
            
            compare(expected, changeLog.getChangeSet());
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    private void compare(final Collection<IChangeRecord> expected,
            final Collection<IChangeRecord> actual) {
        
        fail();
        
    }

    /**
     * This is a very simple implementation of a change log.  NOTE: This is not
     * a particularly great implementation.  First of all it ends up storing
     * two copies of the change set.  Secondly it needs to be smarter about
     * concurrency, or maybe we can be smart about it when we do the
     * implementation on the other side (the SAIL connection can just write
     * change events to a buffer and then the buffer can be drained by 
     * another thread that doesn't block the actual read/write operations,
     * although then we need to be careful not to issue the committed()
     * notification before the buffer is drained).
     * 
     * @author mike
     *
     */
    public class TestChangeLog implements IChangeLog {
        
        private final Map<BigdataStatement,IChangeRecord> uncommitted = 
            new HashMap<BigdataStatement, IChangeRecord>();
        
        private final Map<BigdataStatement,IChangeRecord> committed = 
            new HashMap<BigdataStatement, IChangeRecord>();
        
        public synchronized void changeEvent(final IChangeRecord record) {
            
            uncommitted.put(record.getStatement(), record);
            
        }
        
        public synchronized void transactionCommited() {
        
            committed.clear();
            
            committed.putAll(uncommitted);
            
            uncommitted.clear();
            
        }
        
        public synchronized void transactionAborted() {

            uncommitted.clear();
            
        }
        
        public Collection<IChangeRecord> getChangeSet() {
            
            return committed.values();
            
        }
        
    }
    
}
