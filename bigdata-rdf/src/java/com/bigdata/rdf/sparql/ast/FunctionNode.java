package com.bigdata.rdf.sparql.ast;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;

/**
 * AST node for anything which is neither a constant nor a variable, including
 * math operators, string functions, etc.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class FunctionNode extends ValueExpressionNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends ValueExpressionNode.Annotations {
  
        /**
         * The function URI from the {@link FunctionRegistry}.
         */
        String FUNCTION_URI = FunctionNode.class.getName() + ".functionURI";

        /**
         * The scalar values needed to construct the {@link IValueExpression}.
         * This is a {@link Serializable} {@link Map} with {@link String} keys
         * and {@link Object} values.
         */
        String SCALAR_VALS = FunctionNode.class.getName() + ".scalarVals";

    }

    /**
     * Construct a function node in the AST.
     * 
     * @param functionURI
     *            the function URI. see {@link FunctionRegistry}
     * @param scalarValues
     *            One or more scalar values that are passed to the function
     * @param args
     *            the arguments to the function.
     */
	public FunctionNode(// 
			final URI functionURI, 
	        final Map<String,Object> scalarValues,
			final ValueExpressionNode... args) {
		
		super(args, null/*anns*/);
		
		super.setProperty(Annotations.SCALAR_VALS, scalarValues);
		
		super.setProperty(Annotations.FUNCTION_URI, functionURI);
		
	}
	
    /**
     * Required deep copy constructor.
     */
    public FunctionNode(FunctionNode op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public FunctionNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }
    
    public URI getFunctionURI() {
		
		return (URI) getRequiredProperty(Annotations.FUNCTION_URI);
		
	}

    /**
     * Returns an unmodifiable map because if the scalar values are modified, we
     * need to clear the value expression cache. This is handled correctly by
     * {@link #setScalarValues(Map)}.
     */
    public Map<String, Object> getScalarValues() {

        @SuppressWarnings("unchecked")
        final Map<String, Object> scalarValues = (Map<String, Object>) getProperty(Annotations.SCALAR_VALS);

        if (scalarValues == null) {

            return Collections.emptyMap();

        }

        return Collections.unmodifiableMap(scalarValues);

	}
	
	/**
	 * Overridden to clear the cached value expression.
	 */
	@Override
	public void invalidate() {
	    
	    super.clearProperty(Annotations.VALUE_EXPR);
	    
        super.invalidate();

    }

    /**
     * Return <code>true</code> iff the {@link FunctionNode} makes use of a
     * {@link FunctionRegistry#BOUND} operator.
     * 
     * @return <code>true</code>iff it uses <code>BOUND()</code>
     * 
     * TODO Unit test.
     */
    public boolean isBound() {

        if (FunctionRegistry.BOUND.equals(getFunctionURI()))
            return true;

        final int arity = arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = get(i);

            if (child instanceof FunctionNode) {

                if (!((FunctionNode) child).isBound()) {

                    return true;

                }

            }

        }

        return false;
        
    }

    /*
     * Factory methods.
     */

    /**
     * Return <code>SameTerm(t1,t2)</code>
     */
    static public FunctionNode sameTerm(final TermNode t1, final TermNode t2) {

        return new FunctionNode(FunctionRegistry.EQ, null/* scalarValues */,
                new ValueExpressionNode[] { t1, t2 });

    }

}
