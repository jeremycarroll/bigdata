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
package com.bigdata.rdf.internal.constraints;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.datatype.XMLGregorianCalendar;

import org.semanticweb.yars.nx.namespace.XSD;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.XSDIntIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * A date expression involving a left IValueExpression operand. The operation to be applied to the operands is specified by the {@link Annotations#OP}
 * annotation.
 */
public class DateBOp extends IVValueExpression<IV> implements INeedsMaterialization{

    /**
	 *
	 */
    private static final long serialVersionUID = 9136864442064392445L;

    public interface Annotations extends ImmutableBOp.Annotations {

        /**
         * The operation to be applied to the left operands (required). The value of this annotation is a {@link DateOp}, such as {@link DateOp#YEAR}.
         *
         * @see DateOp
         */
        String OP = (DateBOp.class.getName() + ".op").intern();
        
    }

    public enum DateOp {
        YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TIMEZONE;

    }

    /**
     *
     * @param left
     *            The left operand.
     * @param right
     *            The right operand.
     * @param op
     *            The annotation specifying the operation to be performed on those operands.
     */
    public DateBOp(final IValueExpression<? extends IV> left, final DateOp op) {

        this(new BOp[] { left }, NV.asMap(new NV(Annotations.OP, op)));

    }

    /**
     * Required shallow copy constructor.
     *
     * @param args
     *            The operands.
     * @param op
     *            The operation.
     */
    private DateBOp(final BOp[] args, final DateOp op) {
        this(args, NV.asMap(new NV(Annotations.OP, op)));
    }

    /**
     * Required shallow copy constructor.
     *
     * @param args
     *            The operands.
     * @param op
     *            The operation.
     */
    public DateBOp(final BOp[] args, Map<String, Object> anns) {

        super(args, anns);

        if (args.length != 1 || args[0] == null || getProperty(Annotations.OP) == null) {

            throw new IllegalArgumentException();

        }

    }

    /**
     * Required deep copy constructor.
     *
     * @param op
     */
    public DateBOp(final DateBOp op) {

        super(op);

    }

    final public IV get(final IBindingSet bs) {

        final IV left = left().get(bs);

        // not yet bound?
        if (left == null)
            throw new SparqlTypeErrorException.UnboundVarException();
        
        
        if (left.isLiteral()) {
            if(!left.isInline()&&!left.hasValue()){
                throw new NotMaterializedException();
            }
                
            BigdataLiteral bl = (BigdataLiteral) left.getValue();
            if (XSD.DATETIME.equals(bl.getDatatype())) {
                XMLGregorianCalendar cal=bl.calendarValue();
                switch (op()) {
                case DAY:
                    return new XSDIntIV(cal.getDay());
                case MONTH:
                    return new XSDIntIV(cal.getMonth());
                case YEAR:
                    return new XSDIntIV(cal.getYear());
                case HOURS:
                    return new XSDIntIV(cal.getHour());
                case SECONDS:
                    return new XSDIntIV(cal.getSecond());
                case MINUTES:
                    return new XSDIntIV(cal.getMinute());
                case TIMEZONE:
                    return new XSDIntIV(cal.getTimezone());
                default:
                    throw new UnsupportedOperationException();
                }
            }
        }
        throw new SparqlTypeErrorException();
    }

    public IValueExpression<? extends IV> left() {
        return get(0);
    }

    public DateOp op() {
        return (DateOp) getRequiredProperty(Annotations.OP);
    }

    public String toString() {

        final StringBuilder sb = new StringBuilder();
        sb.append(op());
        sb.append("(").append(left()).append(")");
        return sb.toString();

    }

    @Override
    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }
    
    volatile transient Set<IVariable<IV>> terms;

    public Set<IVariable<IV>> getTermsToMaterialize() {

        if (terms == null) {

            terms = new LinkedHashSet<IVariable<IV>>();

            for (BOp bop : args()) {

                if (bop instanceof IVariable){
                    terms.add((IVariable<IV>) bop);
                }

            }

        }
        return terms;

    }
}
