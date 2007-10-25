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
package com.bigdata.rdf.inf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.TestMagicSets.MagicRule;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.Justification;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.spo.SPOBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Adds support for RDFS inference.
 * <p>
 * A fact always has the form:
 * 
 * <pre>
 * triple(s, p, o)
 * </pre>
 * 
 * where s, p, and or are identifiers for RDF values in the terms index. Facts
 * are stored either in the long-term database or in a per-query answer set.
 * <p>
 * A rule always has the form:
 * 
 * <pre>
 *                       pred :- pred*.
 * </pre>
 * 
 * where <i>pred</i> is either
 * <code>magic(triple(varOrId,varOrId,varOrId))</code> or
 * <code>triple(varOrId,varOrId,varOrId)</code>. A rule is a clause
 * consisting of a head (a predicate) and a body (one or more predicates). Note
 * that the body of the rule MAY be empty. When there are multiple predicates in
 * the body of a rule the rule succeeds iff all predicates in the body succeed.
 * When a rule succeeds, the head of the clause is asserted. If the head is a
 * predicate then it is asserted into the rule base for the query. If it is a
 * fact, then it is asserted into the database for the query. Each predicate has
 * an "arity" with is the number of arguments, e.g., the predicate "triple" has
 * an arity of 3 and may be written as triple/3 while the predicate "magic" has
 * an arity of 1 and may be written as magic/1.
 * <p>
 * A copy is made of the basic rule base at the start of each query and a magic
 * transform is applied to the rule base, producing a new rule base that is
 * specific to the query. Each query is also associated with an answer set in
 * which facts are accumulated. Query execution consists of iteratively applying
 * all rules in the rule base. Execution terminates when no new facts or rules
 * are asserted in a given iteration - this is the <i>fixed point</i> of the
 * query.
 * <p>
 * Note: it is also possible to run the rule set without creating a magic
 * transform. This will produce the full forward closure of the entailments.
 * This is done by using the statements loaded from some source as the source
 * fact base and inserting the entailments created by the rules back into
 * statement collection. When the rules reach their fixed point, the answer set
 * contains both the told triples and the inferred triples and is simply
 * inserted into the long-term database.
 * <p>
 * rdfs9 is represented as:
 * 
 * <pre>
 *                        triple(?v,rdf:type,?x) :-
 *                           triple(?u,rdfs:subClassOf,?x),
 *                           triple(?v,rdf:type,?u). 
 * </pre>
 * 
 * rdfs11 is represented as:
 * 
 * <pre>
 *                        triple(?u,rdfs:subClassOf,?x) :-
 *                           triple(?u,rdfs:subClassOf,?v),
 *                           triple(?v,rdf:subClassOf,?x). 
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME refactor the rules to make them more flexible, but first collect some
 * performance targets 1st on a few data sets using fast and full closure
 * without rdsf4 sot hat I can know how much this hurts or helps. Use TaskATest
 * and setup the inference engine w/o storing ( x type resource ).  We do need
 * the proofs, so store those -- but I can also turn that off to get a baseline
 * for a version with no proofs and magic/SLD.
 * 
 * FIXME Provide incremental closure of data sets are they are loaded.
 * <p>
 * The {@link Rule} class needs to be modified to accept a "new" (statements
 * being loaded or removed from the database) and "db" (either the database
 * (during TM for statement removal) or the union of the new and the database
 * (when adding statements)) parameter and to automatically appy() the rule N
 * times, where N is the #of terms in the tail. In each pass, tail[i] is
 * designated as reading from the "new" data and the other terms in the tail
 * read from the "old" data. This can be done by a variant of apply(). The
 * results are read as a union over the passes, e.g., as an {@link ISPOIterator}.
 * On each pass, the rule should do a rangeCount and execute the individual
 * terms as subqueries where the most selective subquery is run first.
 * <p>
 * 
 * @todo refactor rules to use {@link ISPOIterator}.
 * 
 * @todo refactor rules to isolate each subquery so that we can choose the
 *       execution order dynamically based on the selectivity of the different
 *       subqueries.
 *       <p>
 *       This will also require that we declare the joins, e.g.,
 *       <code>term[i].s = term[j].p</code> so that we can execute the join
 *       correctly regardless of the order in which we execute the subqueries.
 * 
 * @todo refactor rules to define apply() that maps over the terms collecting
 *       the union of the results when executing the rule with each term in turn
 *       reading from the "new" vs the "dbView". Note that the "dbView" is
 *       either just the db or a fused view of the db and "new".
 * 
 * @todo tests of the rule mapping logic and tests of the sub-queries. The rule
 *       execution is now going to be refactored into some logic for choosing
 *       the term execution order and unioning the results, so that can get
 *       tested by itself and then the various rules should work correctly if
 *       they are using the correct triple patterns and join variables.
 * 
 * FIXME truth maintenance (check the SAIL also).
 * 
 * FIXME rdfs:Resource by backward chaining on query. This means that we need a
 * query wrapper for the store or for the inference engine.
 * 
 * FIXME owl:sameAs by backward chaining on query.
 * 
 * @todo We don�t do owl:equivalentClass and owl:equivalentProperty currently.
 *       You can simulate those by doing a bi-directional subClassOf or
 *       subPropertyOf, which has always sufficed for our purposes. If you were
 *       going to write rules for those two things this is how you would do it:
 * 
 * <pre>
 *                           equivalentClass:
 *                           
 *                             add an axiom to the KB: equivalentClass subPropertyOf subClassOf
 *                             add an entailment rule: xxx equivalentClass yyy � yyy equivalentClass xxx
 * </pre>
 * 
 * It would be analogous for equivalentProperty.
 * 
 * @todo Option to turn off range/domain processing.
 * 
 * @todo Improve write efficiency for the proofs - they are slowing things way
 *       down.
 * 
 * @todo provide option for "owl:sameAs" semantics using destructive merging
 *       (the terms are assigned the same term identifier, one of them is
 *       treated as a canonical, and there is no way to retract the sameAs
 *       assertion).
 * 
 * @todo experiment with use of a bloom filter
 * 
 * @todo provide fixed point transitive closure for "chain" rules (subClassOf)
 * 
 * @todo if we are not storing rules or magic facts in the main statement
 *       indices then get rid of the leading byte used in all keys for the
 *       statement indices.
 */
public class InferenceEngine { //implements ITripleStore, IRawTripleStore {

    final public Logger log = Logger.getLogger(InferenceEngine.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Value used for a "NULL" term identifier.
     */
    public static final long NULL = ITripleStore.NULL;

    /**
     * The database that is the authority for the defined terms and term
     * identifiers.
     */
    final protected AbstractTripleStore database;
    
    /**
     * The capacity of the {@link SPOBuffer}.
     * <p>
     * Results on a 45k triple data set:
     * <pre>
     * 1k    - Computed closure in 4469ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=17966
     * 10k   - Computed closure in 3250ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=24704
     * 50k   - Computed closure in 3187ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=25193
     * 100k  - Computed closure in 3359ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=23903
     * 1000k - Computed closure in 3954ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=20306
     * </pre>
     * 
     * Note that the actual performance will depend on the sustained behavior
     * of the JVM, including tuning, and the characteristics of the specific
     * ontology, especially how it influences the #of entailments generated
     * by each rule.  A larger value will be better for sustained closure
     * operations.
     */
    final int BUFFER_SIZE = 200 * Bytes.kilobyte32;
    
    /**
     * Note: making statements distinct in the {@link SPOBuffer} appears to slow
     * things down slightly:
     * 
     * <pre>
     *  
     *  fast: distinct := false;
     *  Computed closure in 3407ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=23566
     *  
     *  fast: distinct := true;
     *  Computed closure in 3594ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=22340
     *  
     *  full: distinct := false;
     *  Computed closure of 12 rules in 3 rounds and 2015ms yeilding 75449 statements total, 29656 inferences, entailmentsPerSec=14717
     *  
     *  full: distinct := true
     *  Computed closure of 12 rules in 3 rounds and 2188ms yeilding 75449 statements total, 29656 inferences, entailmentsPerSec=13553
     * </pre>
     * 
     * FIXME The numbers above were based on the default
     * {@link Object#hashCode()}, which is clearly wrong for this application.
     * I tried to uncomment {@link SPO#hashCode()}, but it slowed things down
     * by 2x. Research the correct way to compute the hash code of three longs
     * and then try this out again!
     */
    final boolean distinct = false;

    /**
     * True iff the Truth Maintenance strategy requires that we store
     * {@link Justification}s for entailments.
     */
    final boolean justify;
    
    /**
     * True iff the Truth Maintenance strategy requires that we store
     * {@link Justification}s for entailments.
     */
    final public boolean isJustified() {
        
        return justify;
        
    }
    
//    /**
//     * The persistent database (vs the temporary store).
//     */
//    public AbstractTripleStore getDatabase() {
//    
//        return database;
//        
//    }
    
    /**
     * Used to assign unique variable identifiers.
     */
    private long nextVar = -1;

    /**
     * Return the next unique variable identifier.
     */
    protected Var nextVar() {

        return new Var(nextVar--);

    }

    /*
     * Identifiers for well-known RDF values. 
     */
    Id rdfType;
    Id rdfProperty;
    Id rdfsSubClassOf;
    Id rdfsSubPropertyOf;
    Id rdfsDomain;
    Id rdfsRange;
    Id rdfsClass;
    Id rdfsResource;
    Id rdfsCMP;
    Id rdfsDatatype;
    Id rdfsMember;
    Id rdfsLiteral;

    /*
     * Rules.
     */
    Rule rdf1;
    Rule rdf2;
    Rule rdfs2;
    Rule rdfs3;
    Rule rdfs4;
    Rule rdfs5;
    Rule rdfs6;
    Rule rdfs7;
    Rule rdfs8;
    Rule rdfs9;
    Rule rdfs10;
    Rule rdfs11;
    Rule rdfs12;
    Rule rdfs13;

    /**
     * Choice of the forward closure algorithm.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static enum ForwardClosureEnum {
        
        /**
         * Runs {@link InferenceEngine#fastForwardClosure()}.
         */
        Fast(),

        /**
         * Runs {@link InferenceEngine#fullForwardClosure()}.
         */
        Full();
        
    }
    
    /**
     * Options for the {@link InferenceEngine}.
     * 
     * @todo Add options:
     *       <p>
     *       Make entailments for rdfs:domain and rdfs:range optional?
     *       <p>
     *       For the {@link #fullForwardClosure()} we can get by with just
     *       Rdfs5, Rdfs7, Rdfs9, and Rdfs11.  Allow the caller to specify
     *       the rule model?
     *       <p>
     *       The {@link SPOBuffer} capacity.
     *       <p>
     *       The {@link InferenceEngine#readService} capacity?
     *       
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Options {
        
//        public static final String TRUTH_MAINENANCE_STRATEGY = "truthMaintenanceStrategy";
//        
//        public static final String DEFAULT_TRUTH_MAINENANCE_STRATEGY = AllProofs.class.getName();

        /**
         * Boolean option - when true the proofs for entailments will be generated and stored in
         * the database.  This option is required by some truth maintenance strategies.
         * 
         * @todo one proof, all proofs, no proofs?
         */
        public static final String JUSTIFY = "justify"; 
        
        public static final String DEFAULT_JUSTIFY = "true"; 
        
        /**
         * Choice of the forward closure algorithm.
         * 
         * @see ForwardClosureEnum
         */
        public static final String FORWARD_CLOSURE = "forwardClosure";

        public static final String DEFAULT_FORWARD_CLOSURE = ForwardClosureEnum.Fast.toString();
        
        /**
         * When true <code>(?x rdf:type rdfs:Resource)</code> entailments are
         * computed and stored in the database. When false, rules that produce
         * those entailments are turned off and the backward chainer will
         * generate the entailments at query time.
         * 
         * @todo implement backward chaining for this and change the default to [false].
         */
        public static final String FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE = "forwardChainRdfTypeRdfsResource";

        public static final String DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE = "true";

        /**
         * @todo document and implement. if we only support this by backward
         *       chaining then get rid of this option.
         * 
         * @todo option for destructive merging for owl:sameAs (collapes term
         *       identifers together)?
         */
        public static final String FORWARD_CHAIN_OWL_SAMEAS= "forwardChainOwlSameAs";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_SAMEAS = "true";
        
//        public static final String FORWARD_CHAINER = "forwardChainer";
//
//        public static final String BACKWARD_CHAINER = "backwardChainer";
        
    }

//    /**
//     * Forward closure is performed when data are loaded into the store and all
//     * proofs are stored.
//     * 
//     * @todo must also determine which rules are forward chained and which are
//     * backward chained.
//     * 
//     * @todo update javadoc. Statement removal for a statement that is no longer
//     *       explicitly asserted simply examines the proofs for that statement.
//     *       If the statement is no longer proven then it is removed and
//     *       dependent statements are tested to see if they are no longer
//     *       provable. This option is the slowest when statements are inserted
//     *       into the store since a large number of proofs objects are
//     *       generated, but it is the fastest for truth maintenance and can pay
//     *       off when statement removal is a common operation for the kb.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class AllProofs implements ITruthMaintenanceStrategy {
//
//        public void createProof(Rule rule, SPO head, SPO[] tail) {
//            // TODO Auto-generated method stub
//            
//        }
//
//        public void createProof(Rule rule, SPO head, long[] tail) {
//            // TODO Auto-generated method stub
//            
//        }
//
//        public int addStatements(ISPOIterator stmts) {
//            // TODO Auto-generated method stub
//            return 0;
//        }
//        
//        public int removeStatements(Resource s, URI p, Value o) {
//            // TODO Auto-generated method stub
//            return 0;
//        }
//        
//    }
    
    /**
     * Configure {@link InferenceEngine} using default {@link Options}.
     * 
     * @param database
     */
    public InferenceEngine(ITripleStore database) {
    
        this(new Properties(),database);
        
    }
    
    /**
     * @param properties
     *            Configuration {@link Options}.
     * @param database
     *            The database for which this class will compute entailments.
     */
    public InferenceEngine(Properties properties, ITripleStore database) {

        if (database == null)
            throw new IllegalArgumentException();

        this.database = (AbstractTripleStore) database;

        this.justify = Boolean.parseBoolean(properties.getProperty(
                Options.JUSTIFY, Options.DEFAULT_JUSTIFY));
        
        this.forwardClosure = ForwardClosureEnum
                .valueOf(properties.getProperty(Options.FORWARD_CLOSURE,
                        Options.DEFAULT_FORWARD_CLOSURE)); 
        
        this.storeRdfTypeRdfsResource = Boolean.parseBoolean(properties
                .getProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,
                        Options.DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE));
        
        setup();

    }
    
    /**
     * Set based on {@link Options#FORWARD_CLOSURE}. 
     */
    final ForwardClosureEnum forwardClosure;
    
    /**
     * Set based on {@link Options#FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE}.
     */
    final protected boolean storeRdfTypeRdfsResource;

    /**
     * Sets up the inference engine.
     */
    protected void setup() {

        setupIds();

        setupRules();

    }

    /**
     * Resolves or defines well-known RDF values.
     * 
     * @see #rdfType and friends which are initialized by this method.
     */
    private void setupIds() {

        _Value rdfType = new _URI(RDF.TYPE);
        _Value rdfProperty = new _URI(RDF.PROPERTY);
        _Value rdfsSubClassOf = new _URI(RDFS.SUBCLASSOF);
        _Value rdfsSubPropertyOf = new _URI(RDFS.SUBPROPERTYOF);
        _Value rdfsDomain = new _URI(RDFS.DOMAIN);
        _Value rdfsRange = new _URI(RDFS.RANGE);
        _Value rdfsClass = new _URI(RDFS.CLASS);
        _Value rdfsResource = new _URI(RDFS.RESOURCE);
        _Value rdfsCMP = new _URI(RDFS.CONTAINERMEMBERSHIPPROPERTY);
        _Value rdfsDatatype = new _URI(RDFS.DATATYPE);
        _Value rdfsMember = new _URI(RDFS.MEMBER);
        _Value rdfsLiteral = new _URI(RDFS.LITERAL);
        
        _Value[] terms = new _Value[]{
        
                rdfType,
                rdfProperty,
                rdfsSubClassOf,
                rdfsSubPropertyOf,
                rdfsDomain,
                rdfsRange,
                rdfsClass,
                rdfsResource,
                rdfsCMP,
                rdfsDatatype,
                rdfsMember,
                rdfsLiteral
                
        };
        
        database.insertTerms(terms, terms.length, false/*haveKeys*/, false/*sorted*/);

        this.rdfType = new Id(rdfType.termId);
        this.rdfProperty = new Id(rdfProperty.termId);
        this.rdfsSubClassOf = new Id(rdfsSubClassOf.termId);
        this.rdfsSubPropertyOf= new Id(rdfsSubPropertyOf.termId);
        this.rdfsDomain = new Id(rdfsDomain.termId);
        this.rdfsRange = new Id(rdfsRange.termId);
        this.rdfsClass = new Id(rdfsClass.termId);
        this.rdfsResource = new Id(rdfsResource.termId);
        this.rdfsCMP = new Id(rdfsCMP.termId);
        this.rdfsDatatype = new Id(rdfsDatatype.termId);
        this.rdfsMember = new Id(rdfsMember.termId);
        this.rdfsLiteral = new Id(rdfsLiteral.termId);

    }

    private void setupRules() {

        rdf1 = new RuleRdf01(this,nextVar(),nextVar(),nextVar());

        // @todo write and initialize rdf2 (?x rdf:type rdf:XMLLiteral).
//        
//        rdf2 = new RuleRdf02(this,nextVar(),nextVar(),nextVar());

        rdfs2 = new RuleRdfs02(this,nextVar(),nextVar(),nextVar(),nextVar());

        rdfs3 = new RuleRdfs03(this,nextVar(),nextVar(),nextVar(),nextVar());

        rdfs4 = new RuleRdfs04(this,nextVar(),nextVar(),nextVar());
        
        rdfs5 = new RuleRdfs05(this,nextVar(),nextVar(),nextVar());

        rdfs6 = new RuleRdfs06(this,nextVar(),nextVar(),nextVar());

        rdfs7 = new RuleRdfs07(this,nextVar(),nextVar(),nextVar(),nextVar());

        rdfs8 = new RuleRdfs08(this,nextVar(),nextVar(),nextVar());

        rdfs9 = new RuleRdfs09(this,nextVar(),nextVar(),nextVar());

        rdfs10 = new RuleRdfs10(this,nextVar(),nextVar(),nextVar());

        rdfs11 = new RuleRdfs11(this,nextVar(),nextVar(),nextVar());

        rdfs12 = new RuleRdfs12(this,nextVar(),nextVar(),nextVar());

        rdfs13 = new RuleRdfs13(this,nextVar(),nextVar(),nextVar());

    }

    /**
     * The full set of RDFS model theory entailments.
     */
    public Rule[] getFullForwardClosureRules() {

        // @todo rdf2
        // @todo datatype entailments
        
        return new Rule[] { rdf1, /*rdf2,*/ rdfs2, rdfs3, rdfs4, rdfs5, rdfs6, rdfs7,
                rdfs8, rdfs9, rdfs10, rdfs11, rdfs12, rdfs13 /* datatype entailments */};

    }
    
    /**
     * Return the rule model to be used by {@link #fullForwardClosure()}.
     * 
     * @todo cache this between runs since it never changes.
     */
    public Rule[] getRuleModel() {

        List<Rule> rules = new LinkedList<Rule>();

        rules.add(rdf1);
        
        /*
         * Note: skipping rdf2: (?u ?a ?l) -> ( _:n rdf:type rdf:XMLLiteral),
         * where ?l is a well-typed XML Literal.
         * 
         * @todo should this be included?
         */

        rules.add(rdfs2);
        
        rules.add(rdfs3);
        
        if(storeRdfTypeRdfsResource) {

            /*
             * Note: skipping rdfs4a (?u ?a ?x) -> (?u rdf:type rdfs:Resource)
             * 
             * Note: skipping rdfs4b (?u ?a ?v) -> (?v rdf:type rdfs:Resource)
             * 
             * @todo make sure that they are back-chained.
             */

            rules.add(rdfs4);
            
        }
        
        rules.add(rdfs5);
        
        rules.add(rdfs6);
        
        rules.add(rdfs7);
        
        /*
         * @todo Should we run this rule or backchain?
         * 
         * [MikeP] I would say generate, but you can backchain (I think?) as
         * long as you have the full closure of the type hierarchy, specifically
         * of ? type Class. If you don't have that (i.e. if you are not
         * calculating the domain and range inferences), then you'd need to
         * recursively backchain the tail too. That is why I did not do more
         * backchaining - the backchain for ? type Resource is super easy
         * because it's never longer than 1 move back.
         */
        rules.add(rdfs8);
        
        rules.add(rdfs9);
        
        rules.add(rdfs10);
        
        rules.add(rdfs11);
        
        rules.add(rdfs12);
        
        rules.add(rdfs13);

        /*
         * Note: The datatype entailment rules are being skipped.
         * 
         * @todo make sure that they are back-chained or add them in here.
         */

        /*
         * @todo owl same as if forward closure used for that.
         */
        
        return rules.toArray(new Rule[rules.size()]);

    }
    
    /**
     * Executor service for rule parallelism.
     * 
     * @todo use for parallel execution of map of new vs old+new over the terms
     *       of a rule.
     * 
     * @todo use for parallel execution of sub-queries.
     * 
     * @todo Note that rules that write entailments on the database statement
     *       MUST coordinate to avoid concurrent modification during traversal
     *       of the statement indices. The chunked iterators go a long way to
     *       addressing this.
     */
    final ExecutorService readService = Executors
            .newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());

    /**
     * Compute the forward closure using the algorithm selected by
     * {@link Options#FORWARD_CLOSURE}.
     * 
     * @return Statistics about the operation.
     */
    public ClosureStats computeClosure() {
        
        switch(forwardClosure) {

        case Fast:
            return fastForwardClosure();
        
        case Full:
            return fullForwardClosure();
        
        default: throw new UnsupportedOperationException();
        
        }
        
    }
    
    /**
     * Compute the complete forward closure of the store using a set-at-a-time
     * inference strategy.
     * <p>
     * The general approach is a series of rounds in which each rule is applied
     * to all data in turn. The rules directly embody queries that cause only
     * the statements which can trigger the rule to be visited. Since most rules
     * require two antecedents, this typically means that the rules are running
     * two range queries and performing a join operation in order to identify
     * the set of rule firings. Entailments computed in each round are fed back
     * into the source against which the rules can match their preconditions, so
     * derived entailments may be computed in a succession of rounds. The
     * process halts when no new entailments are computed in a given round.
     */
    public ClosureStats fullForwardClosure() {

        final long begin = System.currentTimeMillis();
        
        final int nbefore = database.getStatementCount();
        
        // add RDF(S) axioms to the database.
        addRdfsAxioms(database);

        /*
         * This is a buffer that is used to hold entailments so that we can
         * insert them into the indices of the <i>tmpStore</i> using ordered
         * insert operations (much faster than random inserts). The buffer is
         * reused by each rule. The rule assumes that the buffer is empty and
         * just keeps a local counter of the #of entailments that it has
         * inserted into the buffer. When the buffer overflows, those
         * entailments are transfered enmass into the tmp store. The buffer is
         * always flushed after each rule and therefore will have been flushed
         * when this method returns.
         */ 
        final SPOBuffer buffer = new SPOBuffer(database, new DoNotAddFilter(),
                BUFFER_SIZE, distinct, isJustified());

        // do the full forward closure of the database.
        System.err.println(fixedPoint(getRuleModel(), buffer).toString());
       
        final int nafter = database.getStatementCount();
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        ClosureStats closureStats = new ClosureStats();
        
        closureStats.elapsed = elapsed;
        closureStats.numComputed = nafter - nbefore;
        
        return closureStats;        
        
    }

    /**
     * Fast forward closure of the store based on <a
     * href="http://www.cs.iastate.edu/~tukw/waim05.pdf">"An approach to RDF(S)
     * Query, Manipulation and Inference on Databases" by Lu, Yu, Tu, Lin, and
     * Zhang</a>.
     * 
     * @todo When modifying to support incremental TM note that some rules are
     *       "special". In particular {@link AbstractRuleFastClosure_3_5_6_7_9}
     *       accept a set of term identifiers that are specially computed. We
     *       need to pass those rules the term identifiers as computed over the
     *       union of the new and old statements.
     */
    public ClosureStats fastForwardClosure() {

        /*
         * Note: The steps below are numbered with regard to the paper cited in
         * the javadoc above.
         * 
         * Most steps presume that the computed entailments have been added to
         * the database (vs the temp store).
         */

        final int firstStatementCount = database.getStatementCount();

        final long begin = System.currentTimeMillis();

        log.debug("Closing kb with " + firstStatementCount
                + " statements");
        
        /*
         * Entailment buffer.
         */
        final SPOBuffer buffer = new SPOBuffer(database, new DoNotAddFilter(),
                BUFFER_SIZE, distinct, isJustified());

        // 1. add RDF(S) axioms to the database.
        addRdfsAxioms(database); // add to the database.
        
        // 2. Compute P (the set of possible sub properties).
        final Set<Long> P = getSubProperties(database);

        // 3. (?x, P, ?y) -> (?x, rdfs:subPropertyOf, ?y)
        System.err.println("step3: "
                + new RuleFastClosure3(this, nextVar(), nextVar(), P).apply(
                        new RuleStats(), buffer));
        buffer.flush();

        // 4. RuleRdfs05 until fix point (rdfs:subPropertyOf closure).
        System.err.println("rdfs5: "
                + fixedPoint(new Rule[] { rdfs5 },buffer));

        // 4a. Obtain: D,R,C,T.
        final Set<Long> D = getSubPropertiesOf(database, rdfsDomain.id);
        final Set<Long> R = getSubPropertiesOf(database, rdfsRange.id);
        final Set<Long> C = getSubPropertiesOf(database, rdfsSubClassOf.id);
        final Set<Long> T = getSubPropertiesOf(database, rdfType.id);

        // 5. (?x, D, ?y ) -> (?x, rdfs:domain, ?y)
        System.err.println("step5: "
                + new RuleFastClosure5(this, nextVar(), nextVar(), D).apply(
                        new RuleStats(), buffer));
        // Note: deferred buffer.flush() since the next step has no dependency.

        // 6. (?x, R, ?y ) -> (?x, rdfs:range, ?y)
        System.err.println("step6: "
                + new RuleFastClosure6(this, nextVar(), nextVar(), R).apply(
                        new RuleStats(), buffer));
        // Note: deferred buffer.flush() since the next step has no dependency.

        // 7. (?x, C, ?y ) -> (?x, rdfs:subClassOf, ?y)
        System.err.println("step7: "
                + new RuleFastClosure7(this, nextVar(), nextVar(), C).apply(
                        new RuleStats(), buffer));
        // Note: flush buffer before running rdfs11.
        buffer.flush();

        // 8. RuleRdfs11 until fix point (rdfs:subClassOf closure).
        System.err.println("rdfs11: "
                + fixedPoint(new Rule[] { rdfs11 }, buffer));

        // 9. (?x, T, ?y ) -> (?x, rdf:type, ?y)
        System.err.println("step9: "
                + new RuleFastClosure9(this, nextVar(), nextVar(), T).apply(
                        new RuleStats(), buffer));
        buffer.flush();

        // 10. RuleRdfs02
        System.err.println("rdfs2: "+rdfs2.apply(new RuleStats(), buffer).toString());
        buffer.flush();
        
        /*
         * 11. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b) ->
         * (?x, rdf:type, ?b).
         */
        System.err.println("step11: "+new RuleFastClosure11(this, nextVar(), nextVar(),
                nextVar(), nextVar(), nextVar()).apply(new RuleStats(), buffer)
                .toString());
        buffer.flush();
        
        // 12. RuleRdfs03
        System.err.println("rdfs3: "+rdfs3.apply(new RuleStats(), buffer).toString());
        buffer.flush();
        
        /* 13. special rule w/ 3-part antecedent.
         * 
         * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:range, ?b ) ->
         * (?x, rdf:type, ?b )
         */
        System.err.println("step13: "+new RuleFastClosure13(this, nextVar(), nextVar(),
                nextVar(), nextVar(), nextVar()).apply(new RuleStats(), buffer)
                .toString());
        buffer.flush();
        
        if(storeRdfTypeRdfsResource) {
            
            /*
             * 14-15. These steps correspond to rdfs4a and rdfs4b and generate
             * (?x rdf:type rdfs:Resource) entailments. We execute these steps
             * iff we are storing those entailments.
             */

            // 14-15. RuleRdf04
            System.err.println("rdfs4: "+rdfs4.apply(new RuleStats(), buffer).toString());
            buffer.flush();

        }

        // 16. RuleRdf01
        System.err.println("rdf1: "+rdf1.apply(new RuleStats(), buffer).toString());
        buffer.flush();
        
        // 17. RuleRdfs09
        System.err.println("rdfs9: "+rdfs9.apply(new RuleStats(), buffer).toString());
        buffer.flush();
        
        // 18. RuleRdfs10
        System.err.println("rdfs10: "+rdfs10.apply(new RuleStats(), buffer).toString());
        buffer.flush();
        
        // 19. RuleRdfs08.
        System.err.println("rdfs8: "+rdfs8.apply(new RuleStats(), buffer).toString());
        buffer.flush();

        // 20. RuleRdfs13.
        System.err.println("rdfs13: "+rdfs13.apply(new RuleStats(), buffer).toString());
        buffer.flush();
        
        // 21. RuleRdfs06.
        System.err.println("rdfs6: "+rdfs6.apply(new RuleStats(), buffer).toString());
        buffer.flush();
        
        // 22. RuleRdfs07.
        System.err.println("rdfs7: "+rdfs7.apply(new RuleStats(), buffer).toString());
        buffer.flush();
        
        /*
         * Done.
         */
        
        final long elapsed = System.currentTimeMillis() - begin;

        final int lastStatementCount = database.getStatementCount();

        final int inferenceCount = lastStatementCount - firstStatementCount;
        
        if(INFO) {

            log.info("\nComputed closure in "
                            + elapsed
                            + "ms yeilding "
                            + lastStatementCount
                            + " statements total, "
                            + (inferenceCount)
                            + " inferences"
                            + ", entailmentsPerSec="
                            + ((long) (((double) inferenceCount)
                                    / ((double) elapsed) * 1000d)));

        }

        ClosureStats closureStats = new ClosureStats();
        
        closureStats.elapsed = elapsed;
        closureStats.numComputed = inferenceCount;
        
        return closureStats;
        
    }

    /**
     * Computes the fixed point for the {@link #database} using a specified rule
     * set.
     * <p>
     * Note: The buffer will have been flushed when this method returns.
     * 
     * @param database
     *            The database whose entailments will be computed. The
     *            {@link Rule}s will match against statements in this database.
     * 
     * @param rules
     *            The rules to be executed.
     * 
     * @param buffer
     *            This is a buffer that is used to hold entailments so that we
     *            can insert them into the indices of the backing database using
     *            ordered insert operations (much faster than random inserts).
     * 
     * @return Some statistics about the fixed point computation.
     */
    public RuleStats fixedPoint(Rule[] rules, SPOBuffer buffer) {
                
        RuleStats totalStats = new RuleStats();

        final long[] timePerRule = new long[rules.length];
        
        final int[] entailmentsPerRule = new int[rules.length];
        
        final int nrules = rules.length;

        final int firstStatementCount = database.getStatementCount();

        final long begin = System.currentTimeMillis();

        log.debug("Closing kb with " + firstStatementCount
                + " statements");

        int round = 0;

        while (true) {

            final int numEntailmentsBefore = buffer.getBackingStore().getStatementCount();
            
            for (int i = 0; i < nrules; i++) {

                RuleStats ruleStats = new RuleStats();
                
                Rule rule = rules[i];

                int nbefore = ruleStats.numComputed;
                
                rule.apply( ruleStats, buffer );
                
                int nnew = ruleStats.numComputed - nbefore;

                // #of statements examined by the rule.
                int nstmts = ruleStats.stmts1 + ruleStats.stmts2;
                
                long elapsed = ruleStats.elapsed;
                
                timePerRule[i] += elapsed;
                
                entailmentsPerRule[i] = ruleStats.numComputed; // Note: already a running sum.
                
                long stmtsPerSec = (nstmts == 0 || elapsed == 0L ? 0
                        : ((long) (((double) nstmts) / ((double) elapsed) * 1000d)));
                                
                if (DEBUG||true) {
                    log.debug("round# " + round + ", "
                            + rule.getClass().getSimpleName()
                            + ", entailments=" + nnew + ", #stmts1="
                            + ruleStats.stmts1 + ", #stmts2="
                            + ruleStats.stmts2 + ", #subqueries="
                            + ruleStats.numSubqueries1
                            + ", #stmtsExaminedPerSec=" + stmtsPerSec);
                }
                
                totalStats.numComputed += ruleStats.numComputed;
                
                totalStats.elapsed += ruleStats.elapsed;
                
            }

            if(true) {
            
                /*
                 * Show times for each rule so far.
                 */
                StringBuilder sb = new StringBuilder();
                
                sb.append("\n");
                
                sb.append("rule    \tms\t#entms\tentms/ms\n");
                
                for(int i=0; i<timePerRule.length; i++) {
                    
                    sb.append(rules[i].getClass().getSimpleName()
                            + "\t"
                            + timePerRule[i]
                            + "\t"
                            + entailmentsPerRule[i]
                            + "\t"
                            + (timePerRule[i] == 0 ? "N/A" : ""+entailmentsPerRule[i]
                                    / timePerRule[i]));
                    
                    sb.append("\n");
                    
                }

                log.debug(sb.toString());
                
            }
            
            /*
             * Flush the statements in the buffer to the temporary store. 
             */
            buffer.flush();

            final int numEntailmentsAfter = buffer.getBackingStore().getStatementCount();
            
            if ( numEntailmentsBefore == numEntailmentsAfter ) {
                
                // This is the fixed point.
                break;
                
            }
            
            /*
             * Transfer the entailments into the primary store so that derived
             * entailments may be computed.
             */
            final long insertStart = System.currentTimeMillis();

            final int numInserted = numEntailmentsAfter - numEntailmentsBefore;
            
//            final int numInserted = copyStatements(tmpStore,database);

            final long insertTime = System.currentTimeMillis() - insertStart;

            if (DEBUG) {
                StringBuilder debug = new StringBuilder();
                debug.append( "round #" ).append( round ).append( ": " );
                debug.append( totalStats.numComputed ).append( " computed in " );
                debug.append( totalStats.elapsed ).append( " millis, " );
                debug.append( numInserted ).append( " inserted in " );
                debug.append( insertTime ).append( " millis " );
                log.debug( debug.toString() );
            }

            round++;
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        final int lastStatementCount = database.getStatementCount();

        if (INFO) {
        
            final int inferenceCount = lastStatementCount - firstStatementCount;
            
            log.info("\nComputed closure of "+rules.length+" rules in "
                            + (round+1) + " rounds and "
                            + elapsed
                            + "ms yeilding "
                            + lastStatementCount
                            + " statements total, "
                            + (inferenceCount)
                            + " inferences"
                            + ", entailmentsPerSec="
                            + ((long) (((double) inferenceCount)
                                    / ((double) elapsed) * 1000d)));

        }

        return totalStats;

    }

    /**
     * Filter keeps matched triple patterns generated OUT of the database.
     * <p>
     * Note: {@link StatementEnum#Explicit} triples are always rejected by this
     * filter so that explicitly asserted triples will always be stored in the
     * database.
     * <p>
     * Note: {@link StatementEnum#Axiom}s are always rejected by this filter so
     * that they will be stored in the database.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class DoNotAddFilter implements ISPOFilter {

        public DoNotAddFilter() {
        }

        public boolean isMatch(SPO spo) {

            if((spo.s & 0x01L) == 1L) {
                
                /*
                 * Note: Explicitly toss out entailments that would place a
                 * literal into the subject position. These statements can enter
                 * the database via rdfs3 and rdfs4b.
                 */

                return true;
                
            }
            
            if (spo.type == StatementEnum.Explicit
                    || spo.type == StatementEnum.Axiom) {
                
                // Accept all explicit triples or axioms.
                
                return false;
                
            }

            if (!storeRdfTypeRdfsResource && spo.p == rdfType.id
                    && spo.o == rdfsResource.id) {
                
                // reject (?x, rdf:type, rdfs:Resource ) 
                
                return true;
                
            }
            
            // Accept everything else.
            
            return false;
            
        }
        
    }
    
    /**
     * Convert a {@link Set} of term identifiers into a sorted array of term
     * identifiers.
     * <P>
     * Note: When issuing multiple queries against the database, it is generally
     * faster to issue those queries in key order.
     * 
     * @return The sorted term identifiers.
     */
    public long[] getSortedArray(Set<Long> ids) {
        
        int n = ids.size();
        
        long[] a = new long[n];
        
        int i = 0;
        
        for(Long id : ids) {
            
            a[i++] = id;
            
        }
        
        Arrays.sort(a);
        
        return a;
        
    }

    /**
     * Computes the set of possible sub properties of rdfs:subPropertyOf (<code>P</code>).
     * This is used by steps 2-4 in {@link #fastForwardClosure()}.
     * 
     * @param database
     *            The database to be queried.
     * 
     * @return A set containing the term identifiers for the members of P.
     */
    public Set<Long> getSubProperties(AbstractTripleStore database) {

        final Set<Long> P = new HashSet<Long>();
        
        P.add(rdfsSubPropertyOf.id);

        /*
         * query := (?x, P, P), adding new members to P until P reaches fix
         * point.
         */
        {

            int nbefore;
            int nafter = 0;
            int nrounds = 0;

            Set<Long> tmp = new HashSet<Long>();

            do {

                nbefore = P.size();

                tmp.clear();

                /*
                 * query := (?x, p, ?y ) for each p in P, filter ?y element of
                 * P.
                 */

                for (Long p : P) {

                    byte[] fromKey = database.getKeyBuilder().statement2Key(p,
                            NULL, NULL);

                    byte[] toKey = database.getKeyBuilder().statement2Key(
                            p + 1, NULL, NULL);

                    SPO[] stmts = database.getStatements(KeyOrder.POS, fromKey,
                            toKey);

                    for (int i = 0; i < stmts.length; i++) {

                        if (P.contains(stmts[i].o)) {

                            tmp.add(stmts[i].s);

                        }

                    }

                }

                P.addAll(tmp);

                nafter = P.size();

                nrounds++;

            } while (nafter > nbefore);

        }
        
        if(DEBUG){
            
            Set<String> terms = new HashSet<String>();
            
            for( Long id : P ) {
                
                terms.add(database.getTerm(id).term);
                
            }
            
            log.debug("P: "+terms);
        
        }
        
        return P;

    }
    
    /**
     * Query the <i>database</i> for the sub properties of a given property.
     * <p>
     * Pre-condition: The closure of <code>rdfs:subPropertyOf</code> has been
     * asserted on the database.
     * 
     * @param database
     *            The database to be queried.
     * @param p
     *            The term identifier for the property whose sub-properties will
     *            be obtain.
     * 
     * @return A set containing the term identifiers for the sub properties of
     *         <i>p</i>. 
     */
    public Set<Long> getSubPropertiesOf(AbstractTripleStore database, final long p) {

        if(DEBUG) {
            
            log.debug("p="+database.getTerm(p).term);
            
        }
        
        final Set<Long> tmp = new HashSet<Long>();

        /*
         * query := (?x, rdfs:subPropertyOf, p).
         * 
         * Distinct ?x are gathered in [tmp].
         * 
         * Note: This query is two-bound on the POS index.
         */

        byte[] fromKey = database.getKeyBuilder().statement2Key(p,
                rdfsSubPropertyOf.id, NULL);

        byte[] toKey = database.getKeyBuilder().statement2Key(p,
                rdfsSubPropertyOf.id+1, NULL);

        SPO[] stmts = database.getStatements(KeyOrder.POS, fromKey, toKey);

        for( SPO spo : stmts ) {
            
            boolean added = tmp.add(spo.s);
            
            if(DEBUG) {
                
                log.debug(spo.toString(database) + ", added subject="+added);
                
            }
            
        }

        if(DEBUG){
        
            Set<String> terms = new HashSet<String>();
            
            for( Long id : tmp ) {
                
                terms.add(database.getTerm(id).term);
                
            }
            
            log.debug("sub properties: "+terms);
        
        }
        
        return tmp;

    }

    /**
     * Add the axiomatic RDF(S) triples to the store.
     * <p>
     * Note: The termIds are defined with respect to the backing triple store
     * since the axioms will be copied into the store when the closure is
     * complete.
     * 
     * @param database
     *            The store to which the axioms will be added.
     */
    public void addRdfsAxioms(ITripleStore database) {
        
        Axioms axiomModel = RdfsAxioms.INSTANCE;

        StatementBuffer buffer = new StatementBuffer(database, axiomModel
                .getAxioms().size(), true/*distinct*/);
        
        for (Iterator<Axioms.Triple> itr = axiomModel.getAxioms().iterator(); itr
                .hasNext();) {

            Axioms.Triple triple = itr.next();
            
            URI s = triple.getS();
            
            URI p = triple.getP();
            
            URI o = triple.getO();
            
            buffer.add( s, p, o, StatementEnum.Axiom );
            
        }

        // write on the database.
        buffer.flush();
        
    }
    
    /**
     * Copies the statements from the temporary store into the main store using
     * the <strong>same term identifiers</strong>. This method MUST NOT be used
     * unless it is known in advance that the statements in the source are using
     * term identifiers that are consistent with those in the destination.
     * <p>
     * Note: The statements in the source are NOT removed.
     * 
     * @param src
     *            The temporary store (source).
     * 
     * @param dst
     *            The persistent database (destination).
     * 
     * @return The #of statements inserted into the main store (the count only
     *         reports those statements that were not already in the main
     *         store).
     * 
     * FIXME rewrite to buffer a set of ordered statements and then transfer the
     * buffered statements in chunks to the target store by wrapping them in an
     * {@link SPOArrayIterator}. This will let us reuse the existing logic,
     * e.g., for handling statement types.
     */
    static public int copyStatements( final TempTripleStore src, final AbstractTripleStore dst ) {

        /**
         * Copies statements from one index to another. 
         */
        class CopyStatements implements Callable<Long> {

            private final IIndex src;
            private final IIndex dst;
            
            /**
             * @param src
             * @param dst
             */
            CopyStatements(IIndex src, IIndex dst) {
                
                this.src = src;
                
                this.dst = dst;
                
            }
            
            public Long call() throws Exception {
                
                long numInserted = 0;
                
                IEntryIterator it = src.rangeIterator(null, null);
                
                while (it.hasNext()) {

                    it.next();
                    
                    byte[] key = it.getKey();
                    
                    if (!dst.contains(key)) {

                        if(true) throw new UnsupportedOperationException();
                        
                        // FIXME copy the statement type; upgrade as necessary (inferred -> explicit).
                        dst.insert(key, null);
                        
                        numInserted++;
                        
                    }
                    
                }
                
                return numInserted;
            }
            
        };

        List<Callable<Long>> tasks = new LinkedList<Callable<Long>>();
        
        tasks.add( new CopyStatements(src.getSPOIndex(),dst.getSPOIndex()));
        tasks.add( new CopyStatements(src.getPOSIndex(),dst.getPOSIndex()));
        tasks.add( new CopyStatements(src.getOSPIndex(),dst.getOSPIndex()));
        
        final long numInserted;
        
        try {

            final List<Future<Long>> futures = dst.indexWriteService.invokeAll(tasks);

            final long numInserted1 = futures.get(0).get();
            
            final long numInserted2 = futures.get(1).get();
            
            final long numInserted3 = futures.get(2).get();

            assert numInserted1 == numInserted2;
            
            assert numInserted1 == numInserted3;
            
            numInserted = numInserted1;
        
        } catch (Exception ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return (int) numInserted;

    }
    
    /**
     * Accepts a triple pattern and returns the closure over that triple pattern
     * using a magic transform of the RDFS entailment rules.
     * 
     * @param query
     *            The triple pattern.
     * 
     * @param rules
     *            The rules to be applied.
     * 
     * @return The answer set.
     * 
     * @exception IllegalArgumentException
     *                if query is null.
     * @exception IllegalArgumentException
     *                if query is a fact (no variables).
     * 
     * FIXME Magic sets has NOT been implemented -- this method does NOT
     * function.
     */
    public ITripleStore query(Triple query, Rule[] rules) throws IOException {

        if (query == null)
            throw new IllegalArgumentException("query is null");

        if (query.isFact())
            throw new IllegalArgumentException("no variables");

        if (rules == null)
            throw new IllegalArgumentException("rules is null");

        if (rules.length == 0)
            throw new IllegalArgumentException("no rules");
        
        final int nrules = rules.length;

        /*
         * prepare the magic transform of the provided rules.
         */
        
        Rule[] rules2 = new Rule[nrules];
        
        for( int i=0; i<nrules; i++ ) {

            rules2[i] = new MagicRule(this,rules[i]);

        }
        
        /*
         * @todo create the magic seed and insert it into the answer set.
         */
        Magic magicSeed = new Magic(query);

        /*
         * Run the magic transform.
         */
        
        /*
         * @todo support bufferQueue extension for the transient mode or set the
         * default capacity to something larger.  if things get too large
         * then we need to spill over to disk.
         */
        
        ITripleStore answerSet = new TempTripleStore(new Properties());
        
        int lastStatementCount = database.getStatementCount();

        final long begin = System.currentTimeMillis();

        System.err.println("Running query: "+query);

        int nadded = 0;

        while (true) {

            for (int i = 0; i < nrules; i++) {

                Rule rule = rules[i];

                // nadded += rule.apply();
                // rule.apply();

            }

            int statementCount = database.getStatementCount();

            // testing the #of statement is less prone to error.
            if (lastStatementCount == statementCount) {

                //                if( nadded == 0 ) { // should also work.

                // This is the fixed point.
                break;

            }

        }

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Ran query in " + elapsed + "ms; "
                + lastStatementCount + " statements in answer set.");

        return answerSet;
        
    }

}
