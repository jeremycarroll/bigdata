package com.bigdata.bop.rdf.joinGraph;

import java.io.File;
import java.math.BigInteger;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.rio.RDFFormat;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.engine.QueryLog;
import com.bigdata.bop.joinGraph.rto.JoinGraph;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.internal.XSDIntIV;
import com.bigdata.rdf.internal.XSDIntegerIV;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.IsBoundBOp;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.internal.constraints.NotBOp;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.constraints.SameTermBOp;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.model.BigdataLiteral;
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
 * "BSBM" test set.
 * <p>
 * Note: When running large queries, be sure to provide a sufficient heap, set
 * the -server flag, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestJoinGraphOnBSBMData extends AbstractJoinGraphTestCase {

    /**
     * 
     */
    public TestJoinGraphOnBSBMData() {
    }

	/**
	 * @param name
	 */
	public TestJoinGraphOnBSBMData(String name) {
		super(name);
	}

	@Override
	public Properties getProperties() {

		final Properties p = new Properties(super.getProperties());

//		p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
//				.toString());

//		p.setProperty(AbstractTripleStore.Options.QUADS_MODE, "true");
		
		/*
		 * Don't compute closure in the data loader since it does TM, not
		 * database at once closure.
		 */
		p.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.toString());

		return p;

	}

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
	private static final boolean useExistingJournal = true;
	
    private static final long existingPC = 284826; // BSBM 100M

//     private static final long existingPC = 566496; // BSBM 200M

    private static final File existingJournal = new File("/data/bsbm/bsbm_"
            + existingPC + "/bigdata-bsbm.RW.jnl");

    /**
     * The BSBM queries are parameterized based on the data scale. This is set
     * to a product instance which exists for the scale at which this test is
     * being run.
     */
    private String productInstance;
    
    /**
     * The {@link UUID} of a {@link Journal} resource used by this test.
     */
    private static final UUID resourceId = UUID
            .fromString("cb01d320-235d-11e0-ac64-0800200c9a66");

	protected Journal getJournal(final Properties properties) throws Exception {

        final File file;
        if(useExistingJournal){
            /*
             * Use a specific file generated by some external process.
             */
            file = existingJournal;
            namespace = "BSBM_" + existingPC;
            // valid for BSBM 100M or 200M.
            productInstance = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999";
        } else {
            /*
             * Use a persistent file that is generated once and then reused by
             * each test run.
             */
            final File tmpDir = new File(System.getProperty("java.io.tmpdir"));
            final File testDir = new File(tmpDir, "bigdata-tests");
            testDir.mkdirs();
            file = new File(testDir, resourceId + ".jnl");
            namespace = "BSBM_100";
            // valid for BSBM 100
            productInstance = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product22";
        }
        
        properties.setProperty(Journal.Options.FILE, file.toString());

//      properties.setProperty(Journal.Options.BUFFER_MODE,BufferMode.DiskRW.toString());

//      file.delete();

        if (!file.exists()) {

            final Journal jnl = new Journal(properties);

            final String resource = "bigdata-rdf/src/resources/data/bsbm/dataset_pc100.nt";

            final AbstractTripleStore tripleStore = new LocalTripleStore(jnl,
                    namespace, ITx.UNISOLATED, getProperties());

            // Create the KB instance.
            tripleStore.create();

            // Load data.
            final LoadStats loadStats = tripleStore.getDataLoader().loadData(
                    resource, "file:" + resource, RDFFormat.N3);
            
            if (log.isInfoEnabled())
                log.info(loadStats.toString());

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
     * BSBM Q5
     * 
     * <pre>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
     * 
     * SELECT DISTINCT ?product ?productLabel
     * WHERE { 
     *     ?product rdfs:label ?productLabel .
     *     FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> != ?product)
     *     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> bsbm:productFeature ?prodFeature .
     *     ?product bsbm:productFeature ?prodFeature .
     *     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> bsbm:productPropertyNumeric1 ?origProperty1 .
     *     ?product bsbm:productPropertyNumeric1 ?simProperty1 .
     *     FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))
     *     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> bsbm:productPropertyNumeric2 ?origProperty2 .
     *     ?product bsbm:productPropertyNumeric2 ?simProperty2 .
     *     FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170))
     * }
     * ORDER BY ?productLabel
     * LIMIT 5
     * </pre>
     * 
     * Note: There are two predicates which bind variables (origProperty1 and
     * origProperty2) that are not used by the other predicates and therefore do
     * not share any variables which would form "edges" that define joins. In
     * general, a join without shared variables means the cross product of the
     * sources will be materialized and such joins should be run last.
     * <p>
     * However, in this case there are SPARQL FILTERs which (a) use those
     * variables (origProperty1 and origProperty2); and (b) can constrain the
     * query. This means that running the predicates without shared variables
     * and applying the constraints before the tail of the plan can in fact lead
     * to a more efficient join path.
     */
	@SuppressWarnings("unchecked")
    public void test_bsbm_q5() throws Exception {

		QueryLog.logTableHeader();
		
	    final String namespace = getNamespace();
	    
	    final AbstractTripleStore database = getDatabase(namespace);
	    
		/*
		 * Resolve terms against the lexicon.
		 */
		final BigdataValueFactory valueFactory = database.getLexiconRelation()
				.getValueFactory();

        final String rdfs = "http://www.w3.org/2000/01/rdf-schema#";
//        final String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        final String bsbm = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/";

//        final BigdataURI rdfType = valueFactory.createURI(rdf + "type");

        final BigdataURI rdfsLabel = valueFactory.createURI(rdfs + "label");

        final BigdataURI productFeature = valueFactory.createURI(bsbm
                + "productFeature");

        final BigdataURI productPropertyNumeric1 = valueFactory.createURI(bsbm
                + "productPropertyNumeric1");

        final BigdataURI productPropertyNumeric2 = valueFactory.createURI(bsbm
                + "productPropertyNumeric2");

        final BigdataURI product53999 = valueFactory.createURI(productInstance);
		
        final BigdataValue[] terms = new BigdataValue[] { rdfsLabel,
                productFeature, productPropertyNumeric1,
                productPropertyNumeric2, product53999 };

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

		/*
		 * This demonstrates the translation of one of the constraints into a
		 * key-range constraint on the access path.
		 * 
		 * FIXME What is the purpose of RangeBOp#var? Why is it simProperty and
		 * not origProperty
		 * 
		 * FIXME Is the RangeBOp in addition to, or instead of, the original
		 * constraint?
		 * 
		 * [java] PipelineJoin[14](PipelineJoin[13])[ BOp.bopId=14,
		 * PipelineJoin.
		 * 
		 * constraints=[Constraint(EBVBOp(CompareBOp(simProperty1,MINUS
		 * (origProperty1, XSDInteger(120)))[ CompareBOp.op=GT])),
		 * Constraint(EBVBOp(CompareBOp(simProperty1,PLUS(origProperty1,
		 * XSDInteger(120)))[ CompareBOp.op=LT]))],
		 * 
		 * BOp.evaluationContext=ANY,
		 * 
		 * PipelineJoin.predicate=SPOPredicate[7](product=null, TermId(279564U), simProperty1=null)[
		 * 
		 * IPredicate.relationName=[BSBM_284826.spo],
		 * 
		 * IPredicate.timestamp=1299271044298,
		 * 
		 * IPredicate.flags=[KEYS,VALS,READONLY,PARALLEL],
		 * 
		 * SPOPredicate.range=RangeBOp()[ RangeBOp.var=simProperty1,
		 * RangeBOp.from=MINUS(origProperty1, XSDInteger(120)),
		 * RangeBOp.to=PLUS(origProperty1, XSDInteger(120))
		 * 
		 * ], BOp.bopId=7], QueryHints.optimizer=None]
		 */
		final boolean distinct = true;
		final IVariable<?>[] selected;
		final IConstraint[] constraints;
		final IPredicate[] preds;
		final IPredicate p0, p1, p2, p3, p4, p5, p6;
		{
		    final IVariable product = Var.var("product");
		    final IVariable productLabel = Var.var("productLabel"); 
            final IVariable prodFeature = Var.var("prodFeature");
		    final IVariable simProperty1 = Var.var("simProperty1"); 
		    final IVariable simProperty2 = Var.var("simProperty2"); 
		    final IVariable origProperty1 = Var.var("origProperty1"); 
		    final IVariable origProperty2 = Var.var("origProperty2"); 

			selected = new IVariable[] { product, productLabel };
		    
			// The name space for the SPO relation.
			final String[] spoRelation = new String[] { namespace + ".spo" };

//			// The name space for the Lexicon relation.
//			final String[] lexRelation = new String[] { namespace + ".lex" };

			final long timestamp = database.getIndexManager().getLastCommitTime();

			int nextId = 0;

//			?product rdfs:label ?productLabel .
			p0 = new SPOPredicate(new BOp[] {//
			        product,
					new Constant(rdfsLabel.getIV()),
					productLabel//
					},//
					new NV(BOp.Annotations.BOP_ID, nextId++),//
					new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)//
			);
						
//          <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> bsbm:productFeature ?prodFeature .
			p1 = new SPOPredicate(new BOp[] { //
			        new Constant(product53999.getIV()),//
                    new Constant(productFeature.getIV()),//
                    prodFeature//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)
			);
			
//          ?product bsbm:productFeature ?prodFeature .
            p2 = new SPOPredicate(new BOp[] { //
                    product,//
                    new Constant(productFeature.getIV()),//
                    prodFeature//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)
            );
            
//          <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> bsbm:productPropertyNumeric1 ?origProperty1 .
            p3 = new SPOPredicate(new BOp[] { //
                    new Constant(product53999.getIV()),//
                    new Constant(productPropertyNumeric1.getIV()),//
                    origProperty1//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)
            );
            
//          ?product bsbm:productPropertyNumeric1 ?simProperty1 .
            p4 = new SPOPredicate(new BOp[] { //
                    product,//
                    new Constant(productPropertyNumeric1.getIV()),//
                    simProperty1//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(SPOPredicate.Annotations.RANGE, new RangeBOp(//
							origProperty1,// FIXME verify correct var w/ MikeP
							new MathBOp(origProperty1, new Constant(
									new XSDIntegerIV(BigInteger.valueOf(120))),
									MathOp.MINUS),//
							new MathBOp(origProperty1, new Constant(
									new XSDIntegerIV(BigInteger.valueOf(120))),
									MathOp.PLUS)//
							)),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)//
            );

//          <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> bsbm:productPropertyNumeric2 ?origProperty2 .
            p5 = new SPOPredicate(new BOp[] { //
                    new Constant(product53999.getIV()),//
                    new Constant(productPropertyNumeric2.getIV()),//
                    origProperty2//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)
            );

//          ?product bsbm:productPropertyNumeric2 ?simProperty2 .
            p6 = new SPOPredicate(new BOp[] { //
                    product,//
                    new Constant(productPropertyNumeric2.getIV()),//
                    simProperty2//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
					new NV(SPOPredicate.Annotations.RANGE, new RangeBOp(//
							origProperty2,// FIXME verify correct var with MikeP
							new MathBOp(origProperty2, new Constant(
									new XSDIntegerIV(BigInteger.valueOf(170))),
									MathOp.MINUS),//
							new MathBOp(origProperty2, new Constant(
									new XSDIntegerIV(BigInteger.valueOf(170))),
									MathOp.PLUS)//
							)),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)//
            );

            // the vertices of the join graph (the predicates).
            preds = new IPredicate[] { p0, p1, p2, p3, p4, p5, p6 };

            final IValueExpression[] ves = new IValueExpression[] {

                    /*
                     * FILTER
                     * (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances
                     * /dataFromProducer1092/Product53999> != ?product)
                     */
                    new NotBOp(new SameTermBOp(product, new Constant(product53999.getIV()))), //

                    /*
                     * FILTER (?simProperty1 < (?origProperty1 + 120) &&
                     * ?simProperty1 > (?origProperty1 - 120))
                     * 
                     * Note: The AND in the compound filters is typically
                     * optimized out so each of these is represented as its own
                     * IConstraint.
                     */
                    new CompareBOp(new BOp[] {
                            simProperty1,
                            new MathBOp(origProperty1, new Constant(
                                    new XSDIntIV(120)), MathOp.PLUS) }, NV
                            .asMap(new NV[] { new NV(CompareBOp.Annotations.OP,
                                    CompareOp.LT) })),//

                    new CompareBOp(new BOp[] {
                            simProperty1,
                            new MathBOp(origProperty1, new Constant(
                                    new XSDIntIV(120)), MathOp.MINUS) }, NV
                            .asMap(new NV[] { new NV(CompareBOp.Annotations.OP,
                                    CompareOp.GT) })),//

                    /*
                     * FILTER (?simProperty2 < (?origProperty2 + 170) &&
                     * ?simProperty2 > (?origProperty2 - 170))
                     */
                    new CompareBOp(new BOp[] {
                            simProperty2,
                            new MathBOp(origProperty2, new Constant(
                                    new XSDIntIV(170)), MathOp.PLUS) }, NV
                            .asMap(new NV[] { new NV(CompareBOp.Annotations.OP,
                                    CompareOp.LT) })),//

                    new CompareBOp(new BOp[] {
                            simProperty2,
                            new MathBOp(origProperty2, new Constant(
                                    new XSDIntIV(170)), MathOp.MINUS) }, NV
                            .asMap(new NV[] { new NV(CompareBOp.Annotations.OP,
                                    CompareOp.GT) })),//

            };
            
            // the constraints on the join graph.
            constraints = new IConstraint[ves.length];
            for (int i = 0; i < ves.length; i++) {
            	constraints[i] = new SPARQLConstraint(ves[i]);
            }

        }

        /*
         * Run w/o constraints.
         * 
         * Note: There are no solutions for this query against BSBM 100. The
         * optimizer is only providing the fastest path to prove that. We have
         * to use a larger data set if we want to verify the optimizers join
         * path for a query which produces solutions in the data.
		 * 
		 * Note: The optimizer finds the same join path for the BSBM 100, 100M,
		 * and 200M data sets
         */
        if (false) {
            /*
100M: static: ids=[1, 2, 4, 6, 0, 3, 5]

*** round=5, limit=600: paths{in=1,considered=1,out=1}
path   sourceCard  *          f (   out/    in/  read) =    estCard  : sumEstCard  joinPath
     0       2400  *       1.00 (   600/   600/   600) =       2400  :       9066  [ 1  2  0  4  6  3  5 ]

*** Selected join path: [1, 2, 0, 4, 6, 3, 5]
vertex sourceCard  *          f (   out/    in/  read) =    estCard  : sumEstCard
     1         NA  *            (   N/A/   N/A/   N/A) =         16E :         16
     2         16E *     150.00 (   600/     4/ 10921) =       2400  :      13337
     0       2400  *       1.00 (   600/   600/   600) =       2400  :      16337
     4       2400  *       1.00 (   600/   600/   600) =       2400  :      19337
     6       2400  *       1.00 (   600/   600/   600) =       2400  :      22337
     3       2400  *       1.00 (   600/   600/   600) =       2400  :      25337
     5        N/A  *        N/A (   N/A/   N/A/   N/A) =        N/A  :        N/A

test_bsbm_q5 : Total times: static=8741, runtime=8025, delta(static-runtime)=716

200M: static: ids=[1, 2, 4, 6, 0, 3, 5]

*** round=5, limit=600: paths{in=1,considered=1,out=1}
path   sourceCard  *          f (   out/    in/  read) =    estCard  : sumEstCard  joinPath
     0     166410  *       1.00 (   600/   600/   600) =     166410  :     998460  [ 1  2  0  4  6  3  5 ]

test_bsbm_q5 : Total times: static=8871, runtime=8107, delta(static-runtime)=764
             */
			final IPredicate<?>[] runtimeOrder = doTest(distinct, selected,
					preds, null/* constraints */);
			assertEquals("runtimeOrder", new int[] { 1, 2, 0, 4, 6, 3, 5 },
					BOpUtility.getPredIds(runtimeOrder));
        }

        // Run w/ constraints.
        if(true){
        /*
100M: static: ids=[1, 2, 4, 6, 0, 3, 5]
 
*** round=5, limit=600: paths{in=4,considered=4,out=1}
path   sourceCard  *          f (   out/    in/  read) =    estCard  : sumEstCard  joinPath
     0        107  *       1.00 (    27/    27/    27) =        107  :       2541  [ 1  2  4  3  6  5  0 ]
     
     test_bsbm_q5 : Total times: static=7201, runtime=3686, delta(static-runtime)=3515

*** Selected join path: [1, 2, 4, 3, 6, 5, 0]
vertex sourceCard  *          f (   out/    in/  read) =    estCard  : sumEstCard
     1         NA  *            (   N/A/   N/A/   N/A) =         16E :         16
     2         16E *     150.00 (   600/     4/ 10921) =       2400  :      13337
     4       2400  *       1.00 (   600/   600/   600) =       2400  :      16337
     3       2400  *       0.16 (    97/   600/   600) =        387  :      17324
     6        387  *       1.00 (    97/    97/    97) =        387  :      17808
     5        387  *       0.28 (    27/    97/    97) =        107  :      18012
     0        107  *       1.00 (    27/    27/    27) =        107  :      18146

200M: static: ids=[1, 2, 4, 6, 0, 3, 5]

*** round=5, limit=600: paths{in=4,considered=4,out=1}
path   sourceCard  *          f (   out/    in/  read) =    estCard  : sumEstCard  joinPath
     0       1941  *       1.00 (     7/     7/     7) =       1941  :     344799  [ 1  2  4  3  6  5  0 ]

test_bsbm_q5 : Total times: static=7312, runtime=3305, delta(static-runtime)=4007
            
             */
			final IPredicate<?>[] runtimeOrder = doTest(distinct, selected,
					preds, constraints);
			/*
			 * FIXME The RTO produces join paths on some runs which appear to
			 * have no solutions. I've written a unit test for constraint
			 * attachment for the case below, but the constraints appear to be
			 * attached correctly. I've also run the "bad" join path directly
			 * (see below) and it finds the correct #of solutions. This is
			 * pretty weird.
			 */
			// [5, 3, 1, 2, 4, 6, 0] - Ok and faster.
			// [5, 3, 1, 2, 4, 6, 0] - Ok and faster (8828 vs 3621)
			// [5, 6, 0, 2, 1, 4, 3] - no results!!!
			// [5, 6, 0, 2, 1, 4, 3] - again, no results.
			assertEquals("runtimeOrder", new int[] { 1, 2, 4, 3, 6, 5, 0 },
					BOpUtility.getPredIds(runtimeOrder));
        }
        
        if(false){
			// Run some fixed order.
//			final IPredicate<?>[] path = { p5, p6, p0, p2, p1, p4, p3 }; 
//			final IPredicate<?>[] path = { p5, p3, p1, p2, p4, p6, p0 };
			final IPredicate<?>[] path = { p3, p5, p1, p2, p6, p4, p0 };
			runQuery("FIXED ORDER", queryEngine, distinct, selected, path,
					constraints);
		}
        
    }

    /**
     * BSBM Q3
     * 
     * <pre>
     * PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
     * PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * 
     * SELECT ?product ?label
     * WHERE {
     *     ?product rdfs:label ?label .
     *     ?product a %ProductType% .
     *     ?product bsbm:productFeature %ProductFeature1% .
     *     ?product bsbm:productPropertyNumeric1 ?p1 .
     *     FILTER ( ?p1 > %x% ) 
     *     ?product bsbm:productPropertyNumeric3 ?p3 .
     *     FILTER (?p3 < %y% )
     *     OPTIONAL { 
     *         ?product bsbm:productFeature %ProductFeature2% .
     *         ?product rdfs:label ?testVar }
     *     FILTER (!bound(?testVar)) 
     * }
     * ORDER BY ?label
     * LIMIT 10
     * </pre>
     */
    public void test_bsbm_q3() throws Exception {

        fail("This test needs instance data for BSBM 100 and 100M");
        
        QueryLog.logTableHeader();
        
        final String namespace = getNamespace();
        
        final AbstractTripleStore database = getDatabase(namespace);
        
        /*
         * Resolve terms against the lexicon.
         */
        final BigdataValueFactory valueFactory = database.getLexiconRelation()
                .getValueFactory();

        final String rdfs = "http://www.w3.org/2000/01/rdf-schema#";
        final String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        final String bsbm = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/";
        final String bsbmInst ="http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/";

        final BigdataURI rdfType = valueFactory.createURI(rdf + "type");

        final BigdataURI rdfsLabel = valueFactory.createURI(rdfs + "label");

        final BigdataURI productFeature = valueFactory.createURI(bsbm
                + "productFeature");

        final BigdataURI productPropertyNumeric1 = valueFactory.createURI(bsbm
                + "productPropertyNumeric1");

        final BigdataURI productPropertyNumeric3 = valueFactory.createURI(bsbm
                + "productPropertyNumeric3");

        // FIXME parameters
        final BigdataURI productType = valueFactory.createURI(productInstance);
        final BigdataURI productFeature1 = valueFactory.createURI(productInstance);
        final BigdataURI productFeature2 = valueFactory.createURI(productInstance);
        final BigdataLiteral x = valueFactory.createLiteral(productInstance);
        final BigdataLiteral y = valueFactory.createLiteral(productInstance);
        
        final BigdataValue[] terms = new BigdataValue[] { rdfType, rdfsLabel,
                productFeature, productPropertyNumeric1,
                productPropertyNumeric3, productType, productFeature1,
                productFeature2, x, y };

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

        final boolean distinct = false;
        final IVariable<?>[] selected;
        final IConstraint[] constraints;
        final IPredicate[] preds;
        final IPredicate p0, p1, p2, p3, p4, p5, p6;
        final IConstraint c0, c1, c2;
        {
            final IVariable product = Var.var("product");
            final IVariable label = Var.var("label");
            final IVariable p1Var = Var.var("p1");
            final IVariable p3Var = Var.var("p3");
            final IVariable testVar = Var.var("testVar");

            selected = new IVariable[]{product,label};
            
            // The name space for the SPO relation.
            final String[] spoRelation = new String[] { namespace + ".spo" };

//          // The name space for the Lexicon relation.
//          final String[] lexRelation = new String[] { namespace + ".lex" };

            final long timestamp = database.getIndexManager().getLastCommitTime();

            int nextId = 0;

//          ?product rdfs:label ?label .
            p0 = new SPOPredicate(new BOp[] {//
                    product,
                    new Constant(rdfsLabel.getIV()),
                    label//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)//
            );

            // ?product a %ProductType% .
            p1 = new SPOPredicate(new BOp[] {//
                    product,
                    new Constant(rdfType.getIV()),
                    new Constant(productType.getIV())//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)//
            );

            // ?product bsbm:productFeature %ProductFeature1% .
            p2 = new SPOPredicate(new BOp[] {//
                    product,
                    new Constant(productFeature.getIV()),
                    new Constant(productFeature1.getIV())//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)//
            );

            // ?product bsbm:productPropertyNumeric1 ?p1 .
            p3 = new SPOPredicate(new BOp[] {//
                    product,
                    new Constant(productPropertyNumeric1.getIV()),
                    p1Var//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)//
            );

            // ?product bsbm:productPropertyNumeric3 ?p3 .
            p4 = new SPOPredicate(new BOp[] {//
                    product,
                    new Constant(productPropertyNumeric3.getIV()),
                    p3Var//
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)//
            );

            /*
             * FIXME (p5,p6) below in an optional join group!
             */
            
            // ?product bsbm:productFeature %ProductFeature2% .
            p5 = new SPOPredicate(new BOp[] {//
                    product,
                    new Constant(productFeature.getIV()),
                    new Constant(productFeature2.getIV()),
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)//
            );

            // ?product rdfs:label ?testVar }
            p6 = new SPOPredicate(new BOp[] {//
                    product,
                    new Constant(rdfsLabel.getIV()),
                    testVar,
                    },//
                    new NV(BOp.Annotations.BOP_ID, nextId++),//
                    new NV(Annotations.TIMESTAMP, timestamp),//
                    new NV(IPredicate.Annotations.RELATION_NAME, spoRelation)//
            );

            // the vertices of the join graph (the predicates).
            preds = new IPredicate[] { p0, p1, p2, p3, p4, p5, p6 };

            // FILTER ( ?p1 > %x% )
            c0 = new SPARQLConstraint(new CompareBOp(new BOp[] { p1Var,
                    new Constant(x.getIV()) }, NV.asMap(new NV[] { new NV(
                    CompareBOp.Annotations.OP, CompareOp.GT) })));

            // FILTER (?p3 < %y% )
            c1 = new SPARQLConstraint(new CompareBOp(new BOp[] { p3Var,
                    new Constant(y.getIV()) }, NV.asMap(new NV[] { new NV(
                    CompareBOp.Annotations.OP, CompareOp.LT) })));

            // FILTER (!bound(?testVar))
            c2 = new SPARQLConstraint(new NotBOp(new IsBoundBOp(testVar)));
            
            // the constraints on the join graph.
            constraints = new IConstraint[] { c0, c1, c2 };

        }

        /*
         * Run the join graph w/ its constraints (?p1>%x% and ?p3<%y%), but not
         * the optional join group nor its constraint (!bound(?testVar)).
         * 
         * FIXME The optional join group is part of the tail plan and can not be
         * fed into the RTO right now.
         */
		final IPredicate<?>[] runtimeOrder = doTest(distinct, selected, preds,
				new IConstraint[] { c0, c1 });

    }

}
