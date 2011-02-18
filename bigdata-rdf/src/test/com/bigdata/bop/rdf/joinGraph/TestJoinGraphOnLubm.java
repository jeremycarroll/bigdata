package com.bigdata.bop.rdf.joinGraph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.rio.RDFFormat;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.controller.JoinGraph;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.RdfsAxioms;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;

/**
 * Unit tests for runtime query optimization using {@link JoinGraph} and the
 * LUBM U1 data set.
 * <p>
 * Note: When running large queries, be sure to provide a sufficient heap, set
 * the -server flag, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestJoinGraph.java 3918 2010-11-08 21:31:17Z thompsonbry $
 * 
 *          FIXME Look at query performance for runtime versus static
 *          optimization on high volume queries and large data sets. Look at AP
 *          elimination, at I/O, at context switching, etc. There are side
 *          effect from which version of the query is run first so real testing
 *          needs to compensate for those (sync to drop the file cache, cold JVM
 *          to clear the BTree cache, etc).
 * 
 *          FIXME There is now an option to converge onto the hot query
 *          performance. Add an option to drop the file system cache and to
 *          reopen the journal in order to converge on the cold query
 *          performance for the selected join orderings. (Or, either devise a
 *          benchmark which can be used assess the relative performance with
 *          disk IO or use the LUBM benchmark at a data scale which would force
 *          queries to touch the disk (this actually requires a very high data
 *          scale for LUBM since the complex queries are not parameterized and
 *          tend to fully cache the relevant data on their first presentation.)
 * 
 *          FIXME Looks like U1000 Q2 runs into GC OH problems with both the
 *          static and runtime query optimizers. Track down why. Note that Q2
 *          also has problems with cardinality estimate underflow which implies
 *          that it is running for some of the join ordering decisions.
 * 
 *          TODO Does the static versus runtime optimization story change at all
 *          if we all lexicon joins or lexicon materialization?
 * 
 *          TODO What is the overhead for the runtime query optimizer on a big
 *          query? Pretty low, right? (But high compared to the static query
 *          optimizer.)
 */
public class TestJoinGraphOnLubm extends AbstractJoinGraphTestCase {

    /**
     * 
     */
    public TestJoinGraphOnLubm() {
    }

	/**
	 * @param name
	 */
	public TestJoinGraphOnLubm(String name) {
		super(name);
	}

	@Override
	public Properties getProperties() {

		final Properties p = new Properties(super.getProperties());

//		p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
//				.toString());

		/* 
		 * Enable RDFS entailments.
		 */
		p.setProperty(
				com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS,
				RdfsAxioms.class.getName());

		/*
		 * Don't compute closure in the data loader since it does TM, not
		 * database at once closure.
		 */
		p.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.toString());

		return p;

	}

	/**
	 * The {@link UUID} of a {@link Journal} resource used by this test.
	 * 
	 * @todo It would be nice to have a comment for the journal so we could tell
	 *       what was in each one. That would probably be a one time thing, or
	 *       at least something which was linked from the root blocks.
	 * 
	 * @todo Verify that we can correctly open RW and WORM journals without any
	 *       hints.
	 */
	private static final UUID resourceId = UUID.fromString("bb93d970-0cc4-48ca-ba9b-123412683b3d");

	private String namespace;

	protected String getNamespace() {
	    
	    return namespace;
	    
	}
	
    /**
     * When true, the test uses hardcoded access to an existing Journal already
     * loaded with some a larger data set (you need to run against a moderately
     * large data set to assess the relative performance of the static and
     * runtime query optimizers).
     */
	static private final boolean useExistingJournal = false;
	
	protected Journal getJournal(final Properties properties) throws Exception {
	    
        final File file;
        if (useExistingJournal) {
            /*
             * Use a specific file generated by some external process.
             */
            final int nuniv = 50;
            file = new File("/data/lubm/U" + nuniv + "/bigdata-lubm.WORM.jnl");
            namespace = "LUBM_U" + nuniv;
        } else {
            /*
             * Use a persistent file that is generated once and then reused by
             * each test run.
             */
            final File tmpDir = new File(System.getProperty("java.io.tmpdir"));
            final File testDir = new File(tmpDir, "bigdata-tests");
            testDir.mkdirs();
            file = new File(testDir, resourceId + ".jnl");
            namespace = "LUBM_U1";
        }
        
        properties.setProperty(Journal.Options.FILE, file.toString());

//      properties.setProperty(Journal.Options.BUFFER_MODE,BufferMode.DiskRW.toString());
        
        if (!file.exists()) {

            final Journal jnl = new Journal(properties);

            final String[] dataFiles = readFiles(new File(
                    "bigdata-rdf/src/resources/data/lehigh/U1"),
                    new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                            return name.endsWith(".owl");
                        }
                    });

            // And add in the ontology.
            final List<String> tmp = new LinkedList<String>();
            tmp.add("bigdata-rdf/src/resources/data/lehigh/univ-bench.owl");
            tmp.addAll(Arrays.asList(dataFiles));
            final String[] resources = tmp.toArray(new String[tmp.size()]);

            final AbstractTripleStore tripleStore = new LocalTripleStore(jnl,
                    namespace, ITx.UNISOLATED, getProperties());

            // Create the KB instance.
            tripleStore.create();

            // Load LUBM U1, including its ontology, and compute the RDFS
            // closure.
            loadData(tripleStore, resources);

            // Truncate the journal (trim its size).
            jnl.truncate();
            
            // Commit the journal.
            jnl.commit();

            // Close the journal.
            jnl.close();
            
        }

        // Open the test resource.
        return new Journal(properties);

	}
	
	/**
	 * Loads the data into the closureStore and computes the closure.
	 */
	private void loadData(final AbstractTripleStore closureStore,
			final String[] resources) throws Exception {

		final LoadStats totals = new LoadStats();
		
		for (String resource : resources) {

			InputStream is = null;
			String baseURI;

			try {

				is = new FileInputStream(new File(resource));
				baseURI = new File(resource).toURI().toString();

			} catch (FileNotFoundException ex) {

				is = getClass().getResourceAsStream(resource);
				java.net.URL resourceUrl = getClass().getResource(resource);

				// if the resource couldn't be found in the file system
				// and couldn't be found by searching from this class'
				// package (com.bigdata.rdf.rules) as root, then use
				// the class loader to try searching from the root of
				// the JAR itself
				if (resourceUrl == null) {
					is = getClass().getClassLoader().getResourceAsStream(
							resource);
					resourceUrl = getClass().getClassLoader().getResource(
							resource);
				}

				if (resourceUrl == null) {
					log.warn("resource not found [" + resource + "]");
					throw new Exception("FAILURE: resource not found ["
							+ resource + "]");
				}

				// must encode spaces in URL before new URI
				String encodedUrlStr = resourceUrl.toString().replaceAll(" ",
						"%20");
				java.net.URI resourceUri = new java.net.URI(encodedUrlStr);
				baseURI = resourceUri.toString();
			}

			if (is == null) {

				fail("No such file or resource: " + resource);

			}

			final LoadStats tmp;
			try {
			
			    tmp = closureStore.getDataLoader()
					.loadData(is, baseURI, RDFFormat.RDFXML);
			    
            } finally {
                
                is.close();
                
            }
            
			totals.add(tmp);
			
		}

//      if(log.isInfoEnabled())
//    	log.info
		System.out.println(totals.toString());
		
		/*
		 * Compute the database at once closure.
		 */
		final ClosureStats closureStats = closureStore.getInferenceEngine()
				.computeClosure(null/* focusStore */);

//        if(log.isInfoEnabled())
//        	log.info
		System.out.println(closureStats.toString());
		
	}

	/**
	 * LUBM Query 2.
	 * <pre>
	 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
	 * SELECT ?x ?y ?z
	 * WHERE{
	 * 	?x a ub:GraduateStudent .
	 * 	?y a ub:University .
	 * 	?z a ub:Department .
	 * 	?x ub:memberOf ?z .
	 * 	?z ub:subOrganizationOf ?y .
	 * 	?x ub:undergraduateDegreeFrom ?y
	 * }
	 * </pre>
	 * 
	 * @throws Exception
	 */
	public void test_LUBM_Q2() throws Exception {

	    final String namespace = getNamespace();
	    
	    final AbstractTripleStore database = getDatabase(namespace);
	    
		/*
		 * Resolve terms against the lexicon.
		 */
		final BigdataValueFactory f = database.getLexiconRelation()
				.getValueFactory();

		final BigdataURI rdfType = f
				.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		final BigdataURI graduateStudent = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");

		final BigdataURI university = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");

		final BigdataURI department = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

		final BigdataURI memberOf = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

		final BigdataURI subOrganizationOf = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

		final BigdataURI undergraduateDegreeFrom = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom");

		final BigdataValue[] terms = new BigdataValue[] { rdfType,
				graduateStudent, university, department, memberOf,
				subOrganizationOf, undergraduateDegreeFrom };

		// resolve terms.
		database.getLexiconRelation()
				.addTerms(terms, terms.length, true/* readOnly */);

		{
			for (BigdataValue tmp : terms) {
				System.out.println(tmp + " : " + tmp.getIV());
				if (tmp.getIV() == null)
					throw new RuntimeException("Not defined: " + tmp);
			}
		}

		final IPredicate[] preds;
		final IPredicate p0, p1, p2, p3, p4, p5;
		{
			final IVariable<?> x = Var.var("x");
			final IVariable<?> y = Var.var("y");
			final IVariable<?> z = Var.var("z");

			// The name space for the SPO relation.
			final String[] relation = new String[] { namespace + ".spo" };

			final long timestamp = database.getIndexManager().getLastCommitTime();

			int nextId = 0;

			// ?x a ub:GraduateStudent .
			p0 = new SPOPredicate(new BOp[] { x,
					new Constant(rdfType.getIV()),
					new Constant(graduateStudent.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?y a ub:University .
			p1 = new SPOPredicate(new BOp[] { y,
					new Constant(rdfType.getIV()),
					new Constant(university.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?z a ub:Department .
			p2 = new SPOPredicate(new BOp[] { z,
					new Constant(rdfType.getIV()),
					new Constant(department.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?x ub:memberOf ?z .
			p3 = new SPOPredicate(new BOp[] { x,
					new Constant(memberOf.getIV()), z },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?z ub:subOrganizationOf ?y .
			p4 = new SPOPredicate(new BOp[] { z,
					new Constant(subOrganizationOf.getIV()), y },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?x ub:undergraduateDegreeFrom ?y
			p5 = new SPOPredicate(new BOp[] { x,
					new Constant(undergraduateDegreeFrom.getIV()), y },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// the vertices of the join graph (the predicates).
			preds = new IPredicate[] { p0, p1, p2, p3, p4, p5 };
		}

        doTest(preds, null/* constraints */);

	} // LUBM_Q2

	/**
	 * LUBM Query 8
	 * 
	 * <pre>
	 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
	 * SELECT ?x ?y ?z
	 * WHERE{
	 * 	?y a ub:Department .
	 * 	?x a ub:Student;
	 * 		ub:memberOf ?y .
	 * 	?y ub:subOrganizationOf <http://www.University0.edu> .
	 * 	?x ub:emailAddress ?z .
	 * }
	 * </pre>
	 * @throws Exception 
	 */
	public void test_LUBM_Q8() throws Exception {

        final String namespace = getNamespace();

        final AbstractTripleStore database = getDatabase(namespace);

		/*
		 * Resolve terms against the lexicon.
		 */
		final BigdataValueFactory f = database.getLexiconRelation()
				.getValueFactory();

		final BigdataURI rdfType = f
				.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		final BigdataURI department = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");

		final BigdataURI student = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student");

		final BigdataURI memberOf = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf");

		final BigdataURI subOrganizationOf = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf");

		final BigdataURI emailAddress = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress");

		final BigdataURI university0 = f
				.createURI("http://www.University0.edu");

		final BigdataValue[] terms = new BigdataValue[] { rdfType, department,
				student, memberOf, subOrganizationOf, emailAddress, university0 };

		// resolve terms.
		database.getLexiconRelation()
				.addTerms(terms, terms.length, true/* readOnly */);

		{
			for (BigdataValue tmp : terms) {
				System.out.println(tmp + " : " + tmp.getIV());
				if (tmp.getIV() == null)
					throw new RuntimeException("Not defined: " + tmp);
			}
		}

		final IPredicate[] preds;
		final IPredicate p0, p1, p2, p3, p4;
		{
			final IVariable<?> x = Var.var("x");
			final IVariable<?> y = Var.var("y");
			final IVariable<?> z = Var.var("z");

			// The name space for the SPO relation.
			final String[] relation = new String[] { namespace + ".spo" };

            final long timestamp = database.getIndexManager().getLastCommitTime();

			int nextId = 0;

			// ?y a ub:Department .
			p0 = new SPOPredicate(new BOp[] { y,
					new Constant(rdfType.getIV()),
					new Constant(department.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?x a ub:Student;
			p1 = new SPOPredicate(new BOp[] { x,
					new Constant(rdfType.getIV()),
					new Constant(student.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// (?x) ub:memberOf ?y .
			p2 = new SPOPredicate(new BOp[] { x,
					new Constant(memberOf.getIV()), y },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?y ub:subOrganizationOf <http://www.University0.edu> .
			p3 = new SPOPredicate(new BOp[] { y,
					new Constant(subOrganizationOf.getIV()),
					new Constant(university0.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?x ub:emailAddress ?z .
			p4 = new SPOPredicate(new BOp[] { x,
					new Constant(emailAddress.getIV()), z },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// the vertices of the join graph (the predicates).
			preds = new IPredicate[] { p0, p1, p2, p3, p4 };
		}

        doTest(preds, null/* constraints */);
        
	} // LUBM Q8

	/**
	 * LUBM Query 9
	 * <pre>
	 * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
	 * PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
	 * SELECT ?x ?y ?z
	 * WHERE{
	 * 	?x a ub:Student .
	 * 	?y a ub:Faculty .
	 * 	?z a ub:Course .
	 * 	?x ub:advisor ?y .
	 * 	?y ub:teacherOf ?z .
	 * 	?x ub:takesCourse ?z .
	 * }
	 * </pre>
	 * 
	 * @throws Exception
	 */
	public void test_LUBM_Q9() throws Exception {

        final String namespace = getNamespace();

        final AbstractTripleStore database = getDatabase(namespace);

		/*
		 * Resolve terms against the lexicon.
		 */
		final BigdataValueFactory f = database.getLexiconRelation()
				.getValueFactory();

		final BigdataURI rdfType = f
				.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		final BigdataURI student = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student");

		final BigdataURI faculty = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Faculty");

		final BigdataURI course = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");

		final BigdataURI advisor = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor");

		final BigdataURI teacherOf = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf");

		final BigdataURI takesCourse = f
				.createURI("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse");

		final BigdataValue[] terms = new BigdataValue[] { rdfType, student,
				faculty, course, advisor, teacherOf, takesCourse };

		// resolve terms.
		database.getLexiconRelation()
				.addTerms(terms, terms.length, true/* readOnly */);

		{
			for (BigdataValue tmp : terms) {
				System.out.println(tmp + " : " + tmp.getIV());
				if (tmp.getIV() == null)
					throw new RuntimeException("Not defined: " + tmp);
			}
		}

		final IPredicate[] preds;
		final IPredicate p0, p1, p2, p3, p4, p5;
		{
			final IVariable<?> x = Var.var("x");
			final IVariable<?> y = Var.var("y");
			final IVariable<?> z = Var.var("z");

			// The name space for the SPO relation.
			final String[] relation = new String[] { namespace + ".spo" };

            final long timestamp = database.getIndexManager().getLastCommitTime();

			int nextId = 0;

			// ?x a ub:Student .
			p0 = new SPOPredicate(new BOp[] { x,
					new Constant(rdfType.getIV()),
					new Constant(student.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?y a ub:Faculty .
			p1 = new SPOPredicate(new BOp[] { y,
					new Constant(rdfType.getIV()),
					new Constant(faculty.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?z a ub:Course .
			p2 = new SPOPredicate(new BOp[] { z,
					new Constant(rdfType.getIV()),
					new Constant(course.getIV()) },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?x ub:advisor ?y .
			p3 = new SPOPredicate(new BOp[] { x,
					new Constant(advisor.getIV()), y },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?y ub:teacherOf ?z .
			p4 = new SPOPredicate(new BOp[] { y,
					new Constant(teacherOf.getIV()), z },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// ?x ub:takesCourse ?z .
			p5 = new SPOPredicate(new BOp[] { x,
					new Constant(takesCourse.getIV()), z },//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, relation)//
			);

			// the vertices of the join graph (the predicates).
			preds = new IPredicate[] { p0, p1, p2, p3, p4, p5 };
		}

        doTest(preds, null/* constraints */);
        
	} // LUBM_Q9

}
