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
package com.bigdata.rdf.rules;

import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.rdf.inf.Axioms;
import com.bigdata.rdf.inf.BackchainTypeResourceIterator;
import com.bigdata.rdf.inf.BaseAxioms;
import com.bigdata.rdf.inf.ClosureStats;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.inf.OwlAxioms;
import com.bigdata.rdf.inf.RdfsAxioms;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Supports RDFS inference plus optional support for <code>owl:sameAs</code>,
 * <code>owl:equivalentProperty</code>, and <code>owl:equivalentClass</code>.
 * Additional entailments can be introduced using custom rules.
 * <p>
 * The {@link IRule}s are declarative, and it is easy to write new rules. In
 * order for the rules to be used by the {@link InferenceEngine} you need to
 * introduce them into the base class. There are two "programs" used to compute
 * and maintain closure. The "full" closure program is a simple fix point of the
 * RDFS+ entailments, except for the <code> foo rdf:type rdfs:Resource</code>
 * entailments which are normally generated at query time. The "fast" closure
 * program breaks nearly all cycles in the RDFS rules and runs nearly entirely
 * as a sequence of {@link IRule}s, including several custom rules. It is far
 * easier to modify the "full" closure program since any new rules can just be
 * dropped into place. Modifying the "fast" closure program requires careful
 * consideration of the entailments computed at each stage in order to determine
 * where a new rule would fit in. When support for <code>owl:sameAs</code>,
 * etc. processing is enabled, some of the entailments are computed by rules run
 * during forward closure and some of the entailments are computed by rules run
 * at query time.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME test backchain iterator at scale.
 * 
 * @todo verify the code in places where it tests against a specific term
 *       identifer defined in {@link RDFSHelper} and not against the closure of
 *       the subclasses of or the subproperties of that term.
 * 
 * @todo provide declarative rule models for forward chaining so that the rules
 *       may be extended without having to edit the code.
 * 
 * @todo consider support for owl:inverseFunctionalProperty. Are there any other
 *       low hanging fruit there?
 * 
 * @todo Improve write efficiency for the proofs - they are slowing things way
 *       down. Perhaps turn off the range count metadata inside of the B+Tree
 *       for that index? Note that using magic sets or a backward chainer will
 *       let us avoid writing proofs altogether since we can prove whether or
 *       not a statement is still entailed without recourse to reading proofs
 *       chains.
 * 
 * @todo explore an option for "owl:sameAs" semantics using destructive merging
 *       (the terms are assigned the same term identifier, one of them is
 *       treated as a canonical, and there is no way to retract the sameAs
 *       assertion). If you take this approach then you must also re-write all
 *       existing assertions using the term whose term identifier is changed to
 *       be that of another term.
 */
public class InferenceEngine extends RDFSVocabulary {

    final static public Logger log = Logger.getLogger(InferenceEngine.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * True iff the Truth Maintenance strategy requires that we store
     * {@link Justification}s for entailments.
     */
    private final boolean justify;
    
    /**
     * The axiom model used by the inference engine.
     */
    private final BaseAxioms axiomModel;
    
    /**
     * The configured axioms.
     */
    public Axioms getAxioms() {
        
        return axiomModel;
        
    }
    
    /**
     * True iff the Truth Maintenance strategy requires that we store
     * {@link Justification}s for entailments.
     */
    final public boolean isJustified() {
        
        return justify;
        
    }

    /**
     * A filter for keeping certain entailments out of the database. It is
     * configured based on how the {@link InferenceEngine} is configured.
     * 
     * @see DoNotAddFilter
     */
    public final DoNotAddFilter doNotAddFilter;
    
    /**
     * Choice of the forward closure algorithm.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static enum ForwardClosureEnum {
        
        /**
         * The "fast" algorithm breaks several cycles in the RDFS rules and is
         * significantly faster.
         * 
         * @see InferenceEngine#fastForwardClosure(AbstractTripleStore, boolean)
         */
        Fast(),

        /**
         * The "full" algorithm runs the rules as a set to fixed point.
         * 
         * @see InferenceEngine#fullForwardClosure(AbstractTripleStore, boolean)
         */
        Full();
        
    }
    
    /**
     * Options for the {@link InferenceEngine}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {

        /**
         * When <code>true</code> (default {@value #DEFAULT_JUSTIFY}), proof
         * chains for entailments generated by foward chaining are stored in the
         * database. This option is required for truth maintenance when
         * retracting assertion.
         * <p>
         * If you will not be retracting statements from the database then you
         * can specify <code>false</code> for a significant performance boost
         * during writes and a smaller profile on the disk.
         * <p>
         * This option does not effect query performance since the
         * justifications are maintained in a distinct index and are only used
         * when retracting assertions.
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
         * When <code>true</code> (default <code>false</code>)
         * <code>(?x rdf:type rdfs:Resource)</code> entailments are computed
         * AND stored in the database. When <code>false</code>, rules that
         * produce those entailments are turned off such that they are neither
         * computed NOR stored and a backward chainer or magic sets technique
         * must be used to generate the entailments at query time.
         * <p>
         * Note: The default is <code>false</code> since eagerly materializing
         * those entailments takes a lot of time and space.
         * 
         * @see BackchainTypeResourceIterator
         */
        public static final String FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE = "forwardChainRdfTypeRdfsResource";

        public static final String DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE = "false";

        /**
         * When true the rule model will only run rules for RDFS model theory
         * (no OWL) and the OWL axioms will not be defined (default
         * <code>false</code>).
         */
        public static final String RDFS_ONLY = "rdfsOnly";

        public static final String DEFAULT_RDFS_ONLY = "false";
        
        /**
         * When <code>true</code> (default <code>true</code>) the reflexive
         * entailments for <code>owl:sameAs</code> are computed
         * by forward chaining and stored in the database unless
         * {@link #RDFS_ONLY} is used to completely disable those entailments.
         * When <code>false</code> those entailments are not computed and
         * <code>owl:sameAs</code> processing is disabled.
         */
        public static final String FORWARD_CHAIN_OWL_SAMEAS_CLOSURE = "forwardChainOwlSameAsClosure";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_CLOSURE = "true";

        /**
         * When <code>true</code> (default <code>false</code>) the
         * entailments that replication properties between instances that are
         * identified as "the same" using <code>owl:sameAs</code> will be
         * forward chained and stored in the database. When <code>false</code>,
         * rules that produce those entailments are turned off such that they
         * are neither computed NOR stored and the entailments may be accessed
         * at query time using the
         * {@link InferenceEngine#backchainIterator(long, long, long)}.
         * <p>
         * Note: The default is <code>false</code> since those entailments can
         * take up a LOT of space in the store and are expensive to compute
         * during data load. It is a lot easier to compute them dynamically when
         * presented with a specific triple pattern. While more computation is
         * performed if a fill triple scan is frequently requested, that is an
         * unusual case and significantly less data will be stored regardless.
         * 
         * @see InferenceEngine#backchainIterator(long, long,
         *      long)
         */
        public static final String FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES = "forwardChainOwlSameAsProperties";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES = "false";

        /**
         * When <code>true</code> (default <code>true</code>) the
         * entailments for <code>owl:equivilantProperty</code> are computed by
         * forward chaining and stored in the database. When <code>false</code>,
         * rules that produce those entailments are turned off such that they
         * are neither computed NOR stored and a backward chainer or magic sets
         * technique must be used to generate the entailments at query time.
         * 
         * @todo implement backward chaining for owl:equivalentProperty and
         *       compare performance?
         */
        public static final String FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY = "forwardChainOwlEquivalentProperty";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY = "true";

        /**
         * When <code>true</code> (default <code>true</code>) the
         * entailments for <code>owl:equivilantClass</code> are computed by
         * forward chaining and stored in the database. When <code>false</code>,
         * rules that produce those entailments are turned off such that they
         * are neither computed NOR stored and a backward chainer or magic sets
         * technique must be used to generate the entailments at query time.
         * 
         * @todo implement backward chaining for owl:equivalentClass and compare
         *       performance?
         */
        public static final String FORWARD_CHAIN_OWL_EQUIVALENT_CLASS = "forwardChainOwlEquivalentClass";

        public static final String DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_CLASS = "true";

    }

    /**
     * Configure {@link InferenceEngine} using properties used to configure the
     * database.
     * 
     * @param database
     * 
     * @see AbstractTripleStore#getInferenceEngine()
     */
    public InferenceEngine(AbstractTripleStore database) {
    
        this(database.getProperties(), database);
        
    }
    
    /**
     * @param properties
     *            Configuration {@link Options}.
     * @param database
     *            The database for which this class will compute entailments.
     */
    public InferenceEngine(Properties properties, AbstractTripleStore database) {

        super((AbstractTripleStore) database);

        this.justify = Boolean.parseBoolean(properties.getProperty(
                Options.JUSTIFY, Options.DEFAULT_JUSTIFY));
       
        log.info(Options.JUSTIFY+"="+justify);
        
        this.forwardClosure = ForwardClosureEnum
                .valueOf(properties.getProperty(Options.FORWARD_CLOSURE,
                        Options.DEFAULT_FORWARD_CLOSURE)); 

        log.info(Options.FORWARD_CLOSURE+"="+forwardClosure);

        this.forwardChainRdfTypeRdfsResource = Boolean.parseBoolean(properties
                .getProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,
                        Options.DEFAULT_FORWARD_RDF_TYPE_RDFS_RESOURCE));

        log.info(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE + "="
                + forwardChainRdfTypeRdfsResource);

        this.rdfsOnly = Boolean.parseBoolean(properties
                .getProperty(Options.RDFS_ONLY,
                        Options.DEFAULT_RDFS_ONLY));

        log.info(Options.RDFS_ONLY + "=" + rdfsOnly);
        
        if(rdfsOnly) {
            
            this.forwardChainOwlSameAsClosure = false;
            this.forwardChainOwlSameAsProperties = false;
            this.forwardChainOwlEquivalentProperty = false;
            this.forwardChainOwlEquivalentClass = false;
            
        } else {
            
            this.forwardChainOwlSameAsClosure = Boolean.parseBoolean(properties
                    .getProperty(Options.FORWARD_CHAIN_OWL_SAMEAS_CLOSURE,
                            Options.DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_CLOSURE));

            log.info(Options.FORWARD_CHAIN_OWL_SAMEAS_CLOSURE + "="
                    + forwardChainOwlSameAsClosure);

            if(forwardChainOwlSameAsClosure) {

                this.forwardChainOwlSameAsProperties = Boolean.parseBoolean(properties
                    .getProperty(Options.FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES,
                            Options.DEFAULT_FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES));
                
            } else {
                
                this.forwardChainOwlSameAsProperties = false;
                
            }

            log.info(Options.FORWARD_CHAIN_OWL_SAMEAS_CLOSURE + "="
                    + forwardChainOwlSameAsClosure);

            this.forwardChainOwlEquivalentProperty = Boolean
                    .parseBoolean(properties
                            .getProperty(
                                    Options.FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY,
                                    Options.DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY));

            log.info(Options.FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY + "="
                    + forwardChainOwlEquivalentProperty);

            this.forwardChainOwlEquivalentClass = Boolean
                    .parseBoolean(properties.getProperty(
                            Options.FORWARD_CHAIN_OWL_EQUIVALENT_CLASS,
                            Options.DEFAULT_FORWARD_CHAIN_OWL_EQUIVALENT_CLASS));

            log.info(Options.FORWARD_CHAIN_OWL_EQUIVALENT_CLASS + "="
                    + forwardChainOwlEquivalentClass);

        }
        
        // Note: used by the DoNotAddFilter.
        axiomModel = (rdfsOnly ? new RdfsAxioms(database) : new OwlAxioms(
                database));
        
        // Add axioms to the database (writes iff not defined).
        axiomModel.addAxioms();
        
        doNotAddFilter = new DoNotAddFilter(this, axiomModel,
                forwardChainRdfTypeRdfsResource);

    }
    
    /**
     * Set based on {@link Options#FORWARD_CLOSURE}. 
     */
    final protected ForwardClosureEnum forwardClosure;
    
    /**
     * Set based on {@link Options#RDFS_ONLY}. When set, owl:sameAs and friends
     * are disabled and only the RDFS MT entailments are used.
     */
    final protected boolean rdfsOnly;
    
    /**
     * Set based on {@link Options#RDFS_ONLY}. When set, owl:sameAs and friends
     * are disabled and only the RDFS MT entailments are used.
     */
    public final boolean isRdfsOnly() {
        
        return rdfsOnly;
        
    }
    
    /**
     * Set based on {@link Options#FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE}. When
     * <code>true</code> the {@link InferenceEngine} is configured to forward
     * chain and store entailments of the form
     * <code>(x rdf:type rdfs:Resource)</code>. When <code>false</code>,
     * those entailments are computed at query time by
     * {@link #backchainIterator(long, long, long)}.
     */
    final protected boolean forwardChainRdfTypeRdfsResource;

    /**
     * Set based on {@link Options#FORWARD_CHAIN_OWL_SAMEAS_CLOSURE}. When
     * <code>true</code> we will forward chain and store the reflexive and
     * transitive closure of <code>owl:sameAs</code> using
     * {@link RuleOwlSameAs1} and {@link RuleOwlSameAs2}.
     * <p>
     * Note: When <code>false</code>, NO owl:sameAs processing will be
     * performed since there is no privision for backward chaining the
     * owl:sameAs closure.
     */
    final protected boolean forwardChainOwlSameAsClosure;

    /**
     * Set based on {@link Options#FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES}. When
     * <code>true</code>, we will forward chain {@link RuleOwlSameAs2} and
     * {@link RuleOwlSameAs3} which replicate properties on individuals
     * identified as the "same" by <code>owl:sameAs</code>. When
     * <code>false</code>, we will compute those entailments at query time in
     * {@link #backchainIterator(long, long, long)}.
     */
    final protected boolean forwardChainOwlSameAsProperties;

    /**
     * Set based on {@link Options#FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY}. When
     * <code>true</code>, we will forward chain and store those entailments.
     * When <code>false</code>, those entailments will NOT be available.
     */
    final protected boolean forwardChainOwlEquivalentProperty;

    /**
     * Set based on {@link Options#FORWARD_CHAIN_OWL_EQUIVALENT_CLASS}. When
     * <code>true</code>, we will forward chain and store those entailments.
     * When <code>false</code>, those entailments will NOT be available.
     */
    final protected boolean forwardChainOwlEquivalentClass;
    
    /**
     * Compute the forward closure of a focusStore against the database using
     * the algorithm selected by {@link Options#FORWARD_CLOSURE}.
     * <p>
     * Note: before calling this method, the caller SHOULD examine the
     * statements in the focusStore and then database. For each statement in the
     * focusStore, if this statement exists explicitly in the database then
     * remove it from the focusStore. If this statement exists implicitly in the
     * database. Regardless of whether the statement was explicit or inferred in
     * the database, remove it from the focusStore. This step prevents the
     * needless (and expensive) reapplication of the rules to data already known
     * to the database!
     * <p>
     * Note: If the <i>focusStore</i> is given, then the entailments will be
     * asserted against the focusStore. Either this method or the caller MUST
     * copy the <i>focusStore</i> onto the database using
     * {@link AbstractTripleStore#copyStatements(AbstractTripleStore, IElementFilter, boolean)}.
     * If you are loading data from some kind of resource, then see
     * {@link DataLoader} which already knows how to do this.
     * <p>
     * See
     * {@link TruthMaintenance#assertAll(com.bigdata.rdf.store.TempTripleStore)},
     * which first handles statements already in the database, then calls this
     * method, and finally copies the remaining explicit statements in the
     * focusStore and the entailments into the database.
     * 
     * @param focusStore
     *            The data set that will be closed against the database
     *            (optional). When <code>null</code> the store will be closed
     *            against itself.
     * 
     * @return Statistics about the operation.
     */
    public ClosureStats computeClosure(AbstractTripleStore focusStore) {
        
        return computeClosure(focusStore, isJustified());
        
    }

    /**
     * This variant allows you to explicitly NOT generate {@link Justification}s
     * for the computed entailments. It is used by the {@link TruthMaintenance}
     * class as part of the algorithm for truth maintenance when retracting
     * statements from the database. It SHOULD NOT be used for any other purpose
     * or you may risk failing to generate justifications.
     * 
     * @param focusStore
     *            The data set that will be closed against the database.
     * @param justify
     *            {@link Justification}s will be generated iff this flag is
     *            <code>true</code>.
     * 
     * @return Statistics about the operation.
     * 
     * @see #computeClosure(AbstractTripleStore)
     */
    public ClosureStats computeClosure(AbstractTripleStore focusStore,
            boolean justify) {

        final MappedProgram program;

        switch (forwardClosure) {

        case Fast:
            program = getFastForwardClosureProgram(
                    //
                    database.getSPORelation().getNamespace(),
                    (focusStore == null ? null : focusStore.getSPORelation()
                            .getNamespace()),//
                    forwardChainRdfTypeRdfsResource, //
                    rdfsOnly,//
                    forwardChainOwlSameAsClosure, //
                    forwardChainOwlSameAsProperties,//
                    forwardChainOwlEquivalentProperty,//
                    forwardChainOwlEquivalentClass//
            );
            break;

        case Full:

            program = getFullClosureProgram(
                    //
                    database.getSPORelation().getNamespace(),
                    (focusStore == null ? null : focusStore.getSPORelation()
                            .getNamespace()),//
                    forwardChainRdfTypeRdfsResource, //
                    rdfsOnly,//
                    forwardChainOwlSameAsClosure, //
                    forwardChainOwlSameAsProperties,//
                    forwardChainOwlEquivalentProperty,//
                    forwardChainOwlEquivalentClass//
            );

            break;

        default:
            throw new AssertionError("fowardClosure=" + forwardClosure);

        }
        
        if(log.isInfoEnabled()) {
            
            log.info("\n\nforwardClosure=" + forwardClosure + ", program="
                    + program);
            
        }
        
        try {

            final long begin = System.currentTimeMillis();

            /*
             * FIXME remove IJoinNexus.RULE once we no longer need the rule to
             * generate the justifications (esp. for scale-out).
             */
            final int solutionFlags = IJoinNexus.ELEMENT//
                    | (justify ? IJoinNexus.RULE | IJoinNexus.BINDINGS : 0)//
//                  | IJoinNexus.RULE  // iff debugging.
                  ;
          
            final RuleContextEnum ruleContext = focusStore == null
            	? RuleContextEnum.DatabaseAtOnceClosure
            	: RuleContextEnum.TruthMaintenance
            	;
            
            final IJoinNexusFactory joinNexusFactory = database
                    .newJoinNexusFactory(ruleContext, ActionEnum.Insert,
                            solutionFlags, doNotAddFilter, justify,
                            false/* backchain */,
                            DefaultEvaluationPlanFactory2.INSTANCE);

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(database
                    .getIndexManager());
            
            final long mutationCount = joinNexus.runMutation(program);

            final long elapsed = System.currentTimeMillis() - begin;
            
            return new ClosureStats(mutationCount,elapsed);

        } catch (Exception ex) {

            throw new RuntimeException(ex);
            
        }
        
    }
    
    /**
     * Return true iff the fully bound statement is an axiom.
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @return
     */
    public boolean isAxiom(long s, long p, long o) {
        
        return axiomModel.isAxiom(s, p, o);
        
    }

    /**
     * Variant that accepts an {@link IPredicate} as a triple pattern.
     * 
     * @param predicate
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public IChunkedOrderedIterator<ISPO> backchainIterator(IPredicate<ISPO> predicate) {
        
        // pass the filter to the server(s)
        return backchainIterator(database.getSPORelation().getAccessPath(predicate));
//        final long s, p, o;
//        {
//
//            final IVariableOrConstant<Long> t = predicate.get(0);
//
//            s = t.isVar() ? NULL : t.get();
//
//        }
//
//        {
//
//            final IVariableOrConstant<Long> t = predicate.get(1);
//
//            p = t.isVar() ? NULL : t.get();
//
//        }
//        
//        {
//
//            final IVariableOrConstant<Long> t = predicate.get(2);
//
//            o = t.isVar() ? NULL : t.get();
//
//        }
//
//        return backchainIterator(s, p, o);

    }
    
    /**
     * Obtain an iterator that will read on the appropriate {@link IAccessPath}
     * for the database and also backchain any entailments for which forward
     * chaining has been turned off, including {@link RuleOwlSameAs2},
     * {@link RuleOwlSameAs3}, and <code>(x rdf:type rdfs:Resource)</code>.
     * 
     * @param s
     *            The subject in triple pattern for that access path.
     * @param p
     *            The predicate in triple pattern for that access path.
     * @param o
     *            The object in triple pattern for that access path.
     * 
     * @return An iterator that will visit the statements in database matching
     *         the triple pattern query plus any necessary entailments.
     */
    public IChunkedOrderedIterator<ISPO> backchainIterator(long s, long p, long o) {
        
        return backchainIterator(s, p, o, null);
        
    }

    /**
     * Obtain an iterator that will read on the appropriate {@link IAccessPath}
     * for the database and also backchain any entailments for which forward
     * chaining has been turned off, including {@link RuleOwlSameAs2},
     * {@link RuleOwlSameAs3}, and <code>(x rdf:type rdfs:Resource)</code>.
     * 
     * @param s
     *            The subject in triple pattern for that access path.
     * @param p
     *            The predicate in triple pattern for that access path.
     * @param o
     *            The object in triple pattern for that access path.
     * 
     * @return An iterator that will visit the statements in database matching
     *         the triple pattern query plus any necessary entailments.
     */
    public IChunkedOrderedIterator<ISPO> backchainIterator(long s, long p,
            long o, IElementFilter<ISPO> filter) {
        
        // pass the filter to the server(s)
        return backchainIterator(database.getAccessPath(s, p, o, filter));
        
    }
    
    /**
     * Obtain an iterator that will read on the appropriate {@link IAccessPath}
     * for the database and also backchain any entailments for which forward
     * chaining has been turned off, including {@link RuleOwlSameAs2},
     * {@link RuleOwlSameAs3}, and <code>(x rdf:type rdfs:Resource)</code>.
     * 
     * @param accessPath
     *            The source {@link IAccessPath}
     * 
     * @return An iterator that will visit the statements in database that would
     *         be visited by that {@link IAccessPath} query plus any necessary
     *         entailments.
     */
    public IChunkedOrderedIterator<ISPO> backchainIterator(IAccessPath<ISPO> accessPath) {

        return new BackchainAccessPath(this, accessPath).iterator();
        
    }
    
}
