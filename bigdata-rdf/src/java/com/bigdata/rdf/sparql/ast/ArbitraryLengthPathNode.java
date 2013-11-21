package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.sparql.ast.PathNode.PathMod;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.optimizers.StaticOptimizer;

/**
 * A special kind of AST node that represents the SPARQL 1.1 arbitrary length
 * path operator. This node has a single child arg - a JoinGroupNode consisting
 * of other operators (the path) that must be run to fixed point. This node also
 * has several annotations that define the schematics (the left and right sides
 * and the lower and upper bounds) of the arbitrary length path.
 */
public class ArbitraryLengthPathNode 
	extends GroupMemberNodeBase<ArbitraryLengthPathNode> 
		implements IBindingProducerNode, IReorderableNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends GroupNodeBase.Annotations {

    	/**
    	 * The left term - can be a variable or a constant.
    	 */
        String LEFT_TERM = Annotations.class.getName() + ".leftTerm";

    	/**
    	 * The right term - can be a variable or a constant.
    	 */
        String RIGHT_TERM = Annotations.class.getName() + ".rightTerm";

        /**
         * The left transitivity variable.
         */
        String TRANSITIVITY_VAR_LEFT = Annotations.class.getName() + ".transitivityVarLeft";

        /**
         * The right transitivity variable.
         */
        String TRANSITIVITY_VAR_RIGHT = Annotations.class.getName() + ".transitivityVarRight";
        
        /**
         * The lower bound on the number of rounds to run.  Can be zero (0) or
         * one (1).  A lower bound of zero is a special kind of path - the
         * Zero Length Path.  A zero length path connects a vertex to itself
         * (in graph parlance).  In the context of arbitrary length paths it
         * means we bind the input onto the output regardless of whether they
         * are actually connected via the path or not.
         */
        String LOWER_BOUND =  Annotations.class.getName() + ".lowerBound";

        /**
         * The upper bound on the number of rounds to run.
         */
        String UPPER_BOUND =  Annotations.class.getName() + ".upperBound";
        
    }
	
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public ArbitraryLengthPathNode(ArbitraryLengthPathNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public ArbitraryLengthPathNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * Fully construct an arbitrary length path node with all required
     * annotations.
     */
    public ArbitraryLengthPathNode(final TermNode left, final TermNode right, 
    		final VarNode transitivityVarLeft, final VarNode transitivityVarRight,
    		final PathMod mod) {
    	this(new BOp[] { new JoinGroupNode() }, NV.asMap(
    			new NV(Annotations.LEFT_TERM, left),
    			new NV(Annotations.RIGHT_TERM, right),
    			new NV(Annotations.TRANSITIVITY_VAR_LEFT, transitivityVarLeft),
    			new NV(Annotations.TRANSITIVITY_VAR_RIGHT, transitivityVarRight),
    			new NV(Annotations.LOWER_BOUND, mod == PathMod.ONE_OR_MORE ? 1L : 0L),
    			new NV(Annotations.UPPER_BOUND, mod == PathMod.ZERO_OR_ONE ? 1L : Long.MAX_VALUE)
    			));    			
    }
    
    /**
     * Returns the left term.
     */
    public TermNode left() {
    	return (TermNode) super.getRequiredProperty(Annotations.LEFT_TERM);
    }
    
    /**
     * Returns the right term.
     */
    public TermNode right() {
    	return (TermNode) super.getRequiredProperty(Annotations.RIGHT_TERM);
    }
    
    /**
     * Return the left transitivity var.
     */
    public VarNode tVarLeft() {
    	return (VarNode) super.getRequiredProperty(Annotations.TRANSITIVITY_VAR_LEFT);
    }
    
    /**
     * Return the right transitivity var.
     */
    public VarNode tVarRight() {
    	return (VarNode) super.getRequiredProperty(Annotations.TRANSITIVITY_VAR_RIGHT);
    }
    
    /**
     * Return the lower bound.
     */
    public long lowerBound() {
    	return (Long) super.getRequiredProperty(Annotations.LOWER_BOUND);
    }
    
    /**
     * Return the upper bound.
     */
    public long upperBound() {
    	return (Long) super.getRequiredProperty(Annotations.UPPER_BOUND);
    }
    
    /**
     * Return the subgroup.
     */
    public JoinGroupNode subgroup() {
    	return (JoinGroupNode) get(0);
    }
    
    /**
     * Return the variables bound by the path - i.e. what this node will
     * attempt to bind when run.
     */
    public Set<IVariable<?>> getProducedBindings() {

        final Set<IVariable<?>> producedBindings = new LinkedHashSet<IVariable<?>>();

        addProducedBindings(left(), producedBindings);
        addProducedBindings(right(), producedBindings);
        
        return producedBindings;

    }
    
    /**
     * This handles the special case where we've wrapped a Var with a Constant
     * because we know it's bound, perhaps by the exogenous bindings.  If we
     * don't handle this case then we get the join vars wrong.
     * 
     * @see StaticAnalysis._getJoinVars
     */
    private void addProducedBindings(final TermNode t, final Set<IVariable<?>> producedBindings) {
    	
    	if (t instanceof VarNode) {
    		
            producedBindings.add(((VarNode) t).getValueExpression());
            
    	} else if (t instanceof ConstantNode) {
    		
    		final ConstantNode cNode = (ConstantNode) t;
    		final Constant<?> c = (Constant<?>) cNode.getValueExpression();
    		final IVariable<?> var = c.getVar();
    		if (var != null) {
    			producedBindings.add(var);
    		}
    		
    	}
    	
    }

	@Override
	public String toString(int indent) {

		final String s = indent(indent);
        
        final StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(s).append(getClass().getSimpleName());
        sb.append("(left=").append(left()).append(", right=").append(right()).append(") {");
        sb.append(subgroup().toString(indent+1));
        sb.append("\n").append(s);

        final Long rangeCount = (Long) getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY);

        if (rangeCount != null) {
            sb.append(indent(indent + 1));
            sb.append(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY);
            sb.append("=");
            sb.append(rangeCount.toString());
            sb.append("\n");
        }
        sb.append("}");
        
        return sb.toString();

	}

	@Override
	public boolean isReorderable() {

		final long estCard = getEstimatedCardinality(null);
		
		return estCard >= 0 && estCard < Long.MAX_VALUE;
		
	}

	@Override
	public long getEstimatedCardinality(StaticOptimizer opt) {
		
		final JoinGroupNode group = subgroup();
		
		long zeroMatchAdjustment = 0;
		/*
		 * if lowerBound() is zero, and both ?s and ?o are
		 * variables then we (notionally) match
		 * any subject or object in the triple store,
		 * see:
		 * 
		 * http://www.w3.org/TR/2013/REC-sparql11-query-20130321/#defn_evalPP_ZeroOrOnePath
		 * 
		 * Despite this not being implemented, the optimizer does better
		 * knowing this correctly.
		 */
		if (lowerBound() == 0 ) {
			int fixedCount = (left() instanceof VarNode ? 1 : 0) + (right() instanceof VarNode ? 1 : 0);
			switch (fixedCount) {
			case 0:
				zeroMatchAdjustment = left().getValue().equals(right().getValue())?1:0;
				break;
			case 1:
				zeroMatchAdjustment = 1;
				break;
			case 2:
				zeroMatchAdjustment =  Long.MAX_VALUE / 2;
				// The following is more accurate, but more expensive and unnecessary.
				// db.getURICount() + db.getBNodeCount(); 
				System.err.println("adj: "+zeroMatchAdjustment);
				break;
			}
		}
		
		/*
		 * Only deal with singleton paths for now.
		 * 
		 * TODO finish the ASTCardinalityOptimizer
		 */
		if (group.arity() == 1) {
			
			final BOp node = group.get(0);
			
			final long estCard = node.getProperty(
					AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, 
					Long.MAX_VALUE); 



			
			return estCard + zeroMatchAdjustment;
			
		}
		
		return Long.MAX_VALUE;		
		
	}

    
}
