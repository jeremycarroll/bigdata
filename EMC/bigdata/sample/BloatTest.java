
package bigdata.sample;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.bigdata.journal.DumpJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RWStrategy;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rwstore.RWStore;

public class BloatTest extends TestCase {

    private static final int NUM_LOOPS = 40;
    private static final int NUM_ITEMS = 10000;
    private static final int NUM_STATIC_STATEMENTS = 13;
    private static final long SLEEP_MILLIS = 10; // was 1000, but no need to sleep when testing journal commitsS

    public void test() throws RepositoryException, InterruptedException, IOException {
        final File jnlFile = new File("/volumes/NonSSD/bigdata/emc/bloat.jnl");

        if (jnlFile.exists()) {
            jnlFile.delete();
        }

        Properties props = new Properties();
        props.setProperty("com.bigdata.rdf.sail.namespace", "topology.kb");
        props.setProperty("com.bigdata.journal.AbstractJournal.bufferMode", "DiskRW");
        props.setProperty("com.bigdata.btree.writeRetentionQueue.capacity", "4000");
        props.setProperty("com.bigdata.btree.BTree.branchingFactor", "128");
        props.setProperty("com.bigdata.service.AbstractTransactionService.minReleaseAge", "1"); // set to zero to avoid mess with history
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.textIndex", "false");
        props.setProperty(
                "com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlTransitiveProperty", "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlSameAsClosure",
                "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlSameAsProperties",
                "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlInverseOf", "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlEquivalentClass",
                "false");
        props.setProperty(
                "com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlEquivalentProperty", "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlHasValue", "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainRdfTypeRdfsResource",
                "false");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.axiomsClass",
                "com.bigdata.rdf.axioms.NoAxioms");
        props.setProperty("com.bigdata.rdf.sail.truthMaintenance", "false");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.justify", "false");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.statementIdentifiers", "false");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.quadsMode", "true");
        props.setProperty("com.bigdata.journal.AbstractJournal.maximumExtent", "209715200");
        props.setProperty("com.bigdata.service.IBigdataClient.collectPlatformStatistics", "false");
        props.setProperty("com.bigdata.service.IBigdataClient.httpdPort", "-1");
        // props.setProperty("com.bigdata.rdf.sail.bufferCapacity", "100000");
        props.setProperty("com.bigdata.rdf.sail.bufferCapacity", "100000");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.bloomFilter", "false");

        props.setProperty(BigdataSail.Options.CREATE_TEMP_FILE, Boolean.FALSE.toString());
        props.setProperty(BigdataSail.Options.FILE, jnlFile.toString());

        BigdataSail sail = new BigdataSail(props);
        Repository repo = new BigdataSailRepository(sail);
        repo.initialize();
        
        final AbstractTripleStore store = sail.getDatabase();
        final Journal jrnl = ((Journal) store.getIndexManager());
        final RWStrategy rwstrat = (RWStrategy) jrnl.getBufferStrategy();
        final RWStore rw = rwstrat.getStore();

        {
            final StringBuilder sb = new StringBuilder();
            sb.append(storeStats(rw));
            //rw.showAllocators(sb);
            
            System.err.println(sb.toString());

        }

        long prevBytes = 0;
        for (int loop = 0; loop < NUM_LOOPS; ++loop) {
            RepositoryConnection conn = repo.getConnection();
            conn.setAutoCommit(false);

            if (loop == 0) {
                applyInitialStatements(conn);
            } else {
            	applyChurningStatements(conn);
            }

            long currBytes = jnlFile.length();
             if (currBytes > prevBytes) {
                System.err.println("Loop " + loop + " journal file is " + currBytes
                        + " bytes <------ JUMP!");
                                
                prevBytes = currBytes;
            }
            conn.commit();
            conn.close();
            
            if (loop % 20 == 0) {
                System.out.println("Loop " + loop + " journal file is " + currBytes + " bytes");
                final StringBuilder sb = new StringBuilder();
                sb.append(storeStats(rw));
                // rw.showAllocators(sb);
                
                System.err.println(sb.toString());

                // dumpJournal(loop, 128, jrnl);
            }
            
            Thread.sleep(SLEEP_MILLIS);
        }
        
        {
            final StringBuilder sb = new StringBuilder();
            sb.append(storeStats(rw));
            rw.showAllocators(sb);
            
            System.err.println(sb.toString());
            // dumpJournal(NUM_LOOPS, 128, jrnl);
        }

    }

    private String storeStats(final RWStore rw) {
        final long allocs = rw.getTotalAllocations();
        final long frees = rw.getTotalFrees();
                
        return "RW allocations: " + allocs + ", frees: " + frees + ", net: " + (allocs-frees) + "\n";
    }
    
    private void applyInitialStatements(RepositoryConnection conn) throws RepositoryException {
        System.out.println("Applying initial statements");
        ValueFactory vf = conn.getValueFactory();
        for (int item = 0; item < NUM_ITEMS; ++item) {
            URI c = vf.createURI("context:item" + item);
            URI s = vf.createURI("subject:item" + item);
            for (int x = 0; x < NUM_STATIC_STATEMENTS; ++x) {
                URI p = vf.createURI("predicate:" + x);
                Literal o = vf.createLiteral("SomeValue");
                conn.add(s, p, o, c);
            }
        }
    }

    private void applyChurningStatements(RepositoryConnection conn) throws RepositoryException {
        ValueFactory vf = conn.getValueFactory();
        for (int item = 0; item < NUM_ITEMS; ++item) {
            URI s = vf.createURI("context:item" + item);
            URI p = vf.createURI("predicate:lastUpdateTime");
            conn.remove(s, p, null);
            conn.add(s, p, vf.createLiteral(System.currentTimeMillis()));
        }

    }

    private void dumpJournal(final int loop, final int bf, final Journal journal) throws IOException {
        final File file = new File("/volumes/NonSSD/bigdata/emc/dumpJournal_loop_" + loop + "_bf_" + bf + ".txt");

        final PrintWriter writer = new PrintWriter(new FileWriter(file));

        final List<String> nsList = new ArrayList<String>();

        nsList.add("topology.kb");

        final DumpJournal dumpJournal = new DumpJournal(journal);

        dumpJournal.dumpJournal(writer, nsList, false, true, true, false);

        writer.close();

        System.err.println("DumpJournal written to " + file.getAbsolutePath());


    }
}
