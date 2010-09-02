package com.bigdata.rdf.rules;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.constraint.INBinarySearch;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.rule.Rule;

/**
 * Rule supporting {@link LocalTripleStore#match(Literal[], URI[], URI)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MatchRule extends Rule<SPO> {

    /**
     * 
     */
    private static final long serialVersionUID = -5002902183499739018L;

    public MatchRule(String relationName, Vocabulary vocab,
            IVariable<IV> lit, IConstant<IV>[] preds,
            IConstant<IV> cls) {

    super(  "matchRule", //
            new SPOPredicate(relationName, var("s"), var("t"), lit), //
            new SPOPredicate[] {
                //
                new SPOPredicate(relationName, var("s"), var("p"), lit),
                //
                new SPOPredicate(new String[] { relationName },
                                -1, // partitionId
                                var("s"), //
                                vocab.getConstant(RDF.TYPE),//
                                var("t"), //
                                null, // context
                                false, //optional
                                ExplicitSPOFilter.INSTANCE,// filter
                                null // expander
                                ),
                //
                new SPOPredicate(relationName, var("t"), vocab
                                .getConstant(RDFS.SUBCLASSOF), cls) //
                },
            new IConstraint[] {
                new INBinarySearch(var("p"), preds) // p IN preds
                });

    }
    
}