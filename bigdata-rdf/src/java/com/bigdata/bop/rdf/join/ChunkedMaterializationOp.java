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
 * Created on Nov 3, 2011
 */

package com.bigdata.bop.rdf.join;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.store.BigdataBindingSetResolverator;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * A vectored materialization operator based on pretty much the same logic as
 * {@link BigdataBindingSetResolverator}. However, this class caches the
 * resolved {@link BigdataValue} reference on the {@link IV} while the
 * {@link BigdataBindingSetResolverator} replaces the {@link IV} in the solution
 * with the {@link BigdataValue}. Also, this class does not filter out variables
 * which are not being materialized.
 */
public class ChunkedMaterializationOp extends PipelineOp {

    private final static Logger log = Logger
            .getLogger(ChunkedMaterializationOp.class);

    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The variables to be materialized. When <code>null</code> or not
         * specified, ALL variables will be materialized. This may not be an
         * empty array as that would imply that there is no need to use this
         * operator.
         */
        String VARS = ChunkedMaterializationOp.class.getName()+".vars";

        String RELATION_NAME = Predicate.Annotations.RELATION_NAME;

        String TIMESTAMP = Predicate.Annotations.TIMESTAMP;
    }

    /**
     * @param args
     * @param annotations
     */
    public ChunkedMaterializationOp(BOp[] args, Map<String, Object> annotations) {

        super(args, annotations);
        
        final IVariable<?>[] vars = (IVariable<?>[]) getProperty(Annotations.VARS);

        if (vars != null && vars.length == 0)
            throw new IllegalArgumentException();

        getRequiredProperty(Annotations.RELATION_NAME);
        
        getRequiredProperty(Annotations.TIMESTAMP);

    }

    /**
     * @param op
     */
    public ChunkedMaterializationOp(ChunkedMaterializationOp op) {

        super(op);
        
    }

    public ChunkedMaterializationOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));

    }

    /**
     * 
     * @param vars
     *            The variables to be materialized. Materialization is only
     *            attempted for those variables which are actually bound in
     *            given solution.
     * @param namespace
     *            The namespace of the {@link LexiconRelation}.
     * @param timestamp
     *            The timestamp against which to read.
     */
    public ChunkedMaterializationOp(final BOp[] args,
            final IVariable<?>[] vars, final String namespace,
            final long timestamp) {
        this(args, //
                new NV(Annotations.VARS, vars),//
                new NV(Annotations.RELATION_NAME, new String[] { namespace }), //
                new NV(Annotations.TIMESTAMP, timestamp) //
        );
    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));

    }

    /**
     * Task executing on the node.
     */
    static private class ChunkTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        /**
         * The variables to be materialized.
         */
        private final IVariable<?>[] vars;

        private final String namespace;

        private final long timestamp;

        ChunkTask(final ChunkedMaterializationOp op,
                final BOpContext<IBindingSet> context) {

            this.context = context;

            this.vars = (IVariable<?>[]) op.getProperty(Annotations.VARS);

            if (vars != null && vars.length == 0)
                throw new IllegalArgumentException();

            namespace = ((String[]) op.getProperty(Annotations.RELATION_NAME))[0];

            timestamp = (Long) op.getProperty(Annotations.TIMESTAMP);

        }

        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            final IAsynchronousIterator<IBindingSet[]> itr = context
                    .getSource();

            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            try {

                final LexiconRelation lex = (LexiconRelation) context
                        .getResource(namespace, timestamp);

                while (itr.hasNext()) {

                    final IBindingSet[] a = itr.next();

                    stats.chunksIn.increment();
                    stats.unitsIn.add(a.length);

                    resolveChunk(vars, lex, a);

                    sink.add(a);

                }

                sink.flush();

                // done.
                return null;

            } finally {

                sink.close();

            }

        }

    } // ChunkTask

    /**
     * Resolve a chunk of {@link IBindingSet}s into a chunk of
     * {@link IBindingSet}s in which {@link IV}s have been resolved to
     * {@link BigdataValue}s.
     * 
     * @param required
     *            The variable(s) to be materialized.
     * @param lex
     *            The lexicon reference.
     * @param chunk
     *            The chunk of solutions whose variables will be materialized.
     */
    static private void resolveChunk(final IVariable<?>[] required,
            final LexiconRelation lex,//
            final IBindingSet[] chunk//
    ) {

        if (log.isInfoEnabled())
            log.info("Fetched chunk: size=" + chunk.length + ", chunk="
                    + Arrays.toString(chunk));

        /*
         * Create a collection of the distinct term identifiers used in this
         * chunk.
         */

        /*
         * Estimate the capacity of the hash map based on the #of variables to
         * materialize per solution and the #of solutions.
         */
        final int initialCapacity = required == null ? chunk.length
                : ((required.length == 0) ? 1 : chunk.length * required.length);

        final Collection<IV<?, ?>> ids = new HashSet<IV<?, ?>>(initialCapacity);

        for (IBindingSet solution : chunk) {

            final IBindingSet bindingSet = solution;

            // System.err.println(solution);

            assert bindingSet != null;

            if (required == null) {

                @SuppressWarnings("rawtypes")
                final Iterator<Map.Entry<IVariable, IConstant>> itr = bindingSet
                        .iterator();

                while (itr.hasNext()) {

                    @SuppressWarnings("rawtypes")
                    final Map.Entry<IVariable, IConstant> entry = itr.next();

                    final IV<?, ?> iv = (IV<?, ?>) entry.getValue().get();

                    if (iv == null) {

                        throw new RuntimeException("NULL? : var="
                                + entry.getKey() + ", " + bindingSet);

                    }

                    ids.add(iv);

                }

            } else {

                for (IVariable<?> v : required) {

                    final IConstant<?> c = bindingSet.get(v);

                    if (c == null) {
                        continue;
                    }

                    final IV<?, ?> iv = (IV<?, ?>) c.get();

                    if (iv == null) {

                        throw new RuntimeException("NULL? : var=" + v + ", "
                                + bindingSet);

                    }

                    ids.add(iv);

                }

            }

        }

        // System.err.println("resolving: " +
        // Arrays.toString(ids.toArray()));

        if (log.isInfoEnabled())
            log.info("Resolving " + ids.size() + " IVs, required="
                    + Arrays.toString(required));

        // batch resolve term identifiers to terms.
        final Map<IV<?, ?>, BigdataValue> terms = lex.getTerms(ids);

        /*
         * Resolve the IVs.
         */
        for (IBindingSet e : chunk) {

            getBindingSet(e, required, terms);

        }

    }

    /**
     * Resolve the term identifiers in the {@link IBindingSet} using the map
     * populated when we fetched the current chunk.
     * 
     * @param bindingSet
     *            A solution whose {@link Long}s will be interpreted as term
     *            identifiers and resolved to the corresponding
     *            {@link BigdataValue}s.
     * 
     * @throws IllegalStateException
     *             if the {@link IBindingSet} was not materialized with the
     *             {@link IBindingSet}.
     */
    static private void getBindingSet(final IBindingSet bindingSet,
            final IVariable<?>[] required,
            final Map<IV<?, ?>, BigdataValue> terms) {

        if (bindingSet == null)
            throw new IllegalArgumentException();

        if (terms == null)
            throw new IllegalArgumentException();

        if(required != null) {

            /*
             * Only the specified variables.
             */
            
            for (IVariable<?> var : required) {

                @SuppressWarnings("unchecked")
                final IConstant<IV<?,?>> c = bindingSet.get(var);

                if (c == null) {

                    continue;

                }

                final IV<?, ?> iv = (IV<?, ?>) c.get();

                if (iv == null) {

                    continue;

                }
                
                final BigdataValue value = terms.get(iv);

                if (value == null) {

                    throw new RuntimeException("Could not resolve: iv=" + iv);

                }

                /*
                 * Replace the binding.
                 * 
                 * FIXME This probably needs to strip out the
                 * BigdataSail#NULL_GRAPH since that should not become bound.
                 */
                ((IV) iv).setValue(value);

            }
            
        } else {
            
            /*
             * Everything in the binding set.
             */
            
            @SuppressWarnings("rawtypes")
            final Iterator<Map.Entry<IVariable, IConstant>> itr = bindingSet
                    .iterator();

            while (itr.hasNext()) {

                @SuppressWarnings("rawtypes")
                final Map.Entry<IVariable, IConstant> entry = itr.next();

                final Object boundValue = entry.getValue().get();

                if (!(boundValue instanceof IV<?, ?>)) {

                    continue;

                }

                final IV<?, ?> iv = (IV<?, ?>) boundValue;

                final BigdataValue value = terms.get(iv);

                if (value == null) {

                    throw new RuntimeException("Could not resolve: iv=" + iv);

                }

                /*
                 * Replace the binding.
                 * 
                 * FIXME This probably needs to strip out the
                 * BigdataSail#NULL_GRAPH since that should not become bound.
                 */
                ((IV) iv).setValue(value);

            }
        }

    }

}
