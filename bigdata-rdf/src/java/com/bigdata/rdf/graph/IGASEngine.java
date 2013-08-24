package com.bigdata.rdf.graph;

/**
 * The interface used to submit an {@link IGASProgram} for evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGASEngine {

    /**
     * Obtain an execution context for the specified {@link IGASProgram}.
     * 
     * @param namespace
     *            The namespace of the graph (KB instance).
     * @param timestamp
     *            The timestamp of the graph view (this should be a read-only
     *            view for non-blocking index reads).
     * @param program
     *            The program to execute against that graph.
     * 
     * @param <VS>
     *            The generic type for the per-vertex state. This is scoped to
     *            the computation of the {@link IGASProgram}.
     * @param <ES>
     *            The generic type for the per-edge state. This is scoped to the
     *            computation of the {@link IGASProgram}.
     * @param <ST>
     *            The generic type for the SUM. This is often directly related
     *            to the generic type for the per-edge state, but that is not
     *            always true. The SUM type is scoped to the GATHER + SUM
     *            operation (NOT the computation).
     */
    <VS, ES, ST> IGASContext<VS, ES, ST> newGASContext(String namespace,
            long timestamp, IGASProgram<VS, ES, ST> program);

    /**
     * Polite shutdown.
     */
    void shutdown();

    /**
     * Immediate shutdown.
     */
    void shutdownNow();
    
}