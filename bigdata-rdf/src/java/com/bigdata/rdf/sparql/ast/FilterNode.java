package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.EBVBOp;

/**
 * AST node models a value expression which imposes a constraint.
 */
public class FilterNode extends GroupMemberValueExpressionNodeBase implements
        IValueExpressionNodeContainer {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Required deep copy constructor.
     */
    public FilterNode(FilterNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public FilterNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }
    
    /**
     * 
     * @param ve
     *            A value expression which places a constraint on the query.
     */
	public FilterNode(final IValueExpressionNode ve) {
		
        super(new BOp[] { (BOp) ve }, null/* anns */);

	}
	
	public IValueExpression<? extends IV> getValueExpression() {

        final IValueExpression<? extends IV> ve = getValueExpressionNode()
                .getValueExpression();
        
        if(ve instanceof IVariable<?>) {
            
            /*
             * Wrap a bare variable in an EBV operator. This is necessary in
             * order for it to properly self-report its materialization
             * requirements. It is also necessary in order for the
             * materialization pipeline to notice that a TermId can not be
             * interpreted as a boolean (there is no problem "getting" the
             * TermId from the variable, but its EBV is undefined until the RDF
             * Value is materialized for that TermId).
             */
            
            return new EBVBOp(ve);
            
        }
        
        return ve;
	    
	}
	
    public IValueExpressionNode getValueExpressionNode() {

        return (IValueExpressionNode) get(0);
        
	}

    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append("\n");
        sb.append(indent(indent));
        sb.append("FILTER( ").append(getValueExpressionNode()).append(" )");

        return sb.toString();

    }

}
