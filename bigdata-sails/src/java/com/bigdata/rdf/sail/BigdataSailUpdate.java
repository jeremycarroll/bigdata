/**
Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.rdf.sail;

import java.util.concurrent.TimeUnit;

import org.openrdf.query.Dataset;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.parser.ParsedUpdate;
import org.openrdf.repository.sail.SailUpdate;

import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Extension API for bigdata queries.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class BigdataSailUpdate extends SailUpdate implements
        BigdataSailOperation {

    private final ASTContainer astContainer;

    public ASTContainer getASTContainer() {
        
        return astContainer;
        
    }

    public BigdataSailUpdate(final ASTContainer astContainer,
            final BigdataSailRepositoryConnection con) {

        super(null/* tupleQuery */, con);

        if (astContainer == null)
            throw new IllegalArgumentException();

        this.astContainer = astContainer;

    }

    public ParsedUpdate getParsedUpdate() {
        
        throw new UnsupportedOperationException();
        
    }

    @Override
    public String toString() {

        return astContainer.toString();
        
    }

    public AbstractTripleStore getTripleStore() {

        return ((BigdataSailRepositoryConnection) getConnection())
                .getTripleStore();

    }

    @Override
    public void setDataset(final Dataset dataset) {

        /*
         * Batch resolve RDF Values to IVs and then set on the query model.
         */

        final Object[] tmp = new BigdataValueReplacer(getTripleStore())
                .replaceValues(dataset, null/* bindings */);

        /*
         * FIXME Set the data set on the original AST, but we have to reconcile
         * the AST models for query and update such that there is a shared
         * interface which supports that operation.
         */
//        astContainer.getOriginalUpdateAST().setDataset(
//                new DatasetNode((Dataset) tmp[0]));
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the "active" dataset for this update. The active dataset is either
     * the dataset that has been specified using {@link #setDataset(Dataset)} or
     * the dataset that has been specified in the update, where the former takes
     * precedence over the latter.
     * 
     * @return The active dataset, or <tt>null</tt> if there is no dataset.
     */
    @Override
    public Dataset getActiveDataset() {

        // FIXME Review DataSet handling in parser, here, and evaluation code. 
//        return astContainer.getOriginalAST().getDataset();
        
//        if (dataset != null) {
//        
//            return dataset;
//
//        }
//
//        // No external dataset specified, use update operation's own dataset (if
//        // any)
//        return parsedUpdate.getDataset();
        
        throw new UnsupportedOperationException();
        
    }

    @Override
    public void execute() throws UpdateExecutionException {

        /*
         * FIXME Pass [includeInferred] through on the AST.
         */
        
//        final QueryRoot originalQuery = astContainer.getOriginalAST();
//
////        if (getMaxQueryTime() > 0)
////            originalQuery.setTimeout(TimeUnit.SECONDS
////                    .toMillis(getMaxQueryTime()));
//
//        originalQuery.setIncludeInferred(getIncludeInferred());

        ASTEvalHelper.executeUpdate(
                ((BigdataSailRepositoryConnection) getConnection()),
                astContainer);

    }

}
