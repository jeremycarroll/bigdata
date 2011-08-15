/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 22, 2009
 */

package com.bigdata.bop.engine;

import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.join.PipelineJoin.PipelineJoinStats;
import com.bigdata.counters.render.XHTMLRenderer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.sail.QueryHints;
import com.bigdata.rdf.sail.Rule2BOpUtility;
import com.bigdata.striterator.IKeyOrder;

/**
 * Class defines the log on which summary operator execution statistics are
 * written.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: RuleLog.java 3448 2010-08-18 20:55:58Z thompsonbry $
 */
public class QueryLog {

    private static final String NA = "N/A";
    private static final String TD = "<td>";
    private static final String TDx = "</td\n>";
    
	protected static final transient Logger log = Logger
            .getLogger(QueryLog.class);

    static {
		logTableHeader();
    }
    
    static public void logTableHeader() {
    	if(log.isInfoEnabled())
    		log.info(QueryLog.getTableHeader());
    }

    /**
     * A single buffer is reused to keep down the heap churn.
     */
	final private static StringBuilder sb = new StringBuilder(
			Bytes.kilobyte32 * 4);

    /**
     * Log rule execution statistics.
     * 
     * @param q
     *            The running query.
     */
    static public void log(final IRunningQuery q) {

		if (log.isInfoEnabled()) {
			
			try {

				/*
				 * Note: We could use a striped lock here over a small pool of
				 * StringBuilder's to decrease contention for the single buffer
				 * while still avoiding heap churn for buffer allocation. Do
				 * this if the monitor for this StringBuilder shows up as a hot
				 * spot when query logging is enabled.
				 */
				synchronized(sb) {
				
					// clear the buffer.
					sb.setLength(0);

					logDetailRows(q, sb);

					logSummaryRow(q, sb);

					log.info(sb);

				}
				
			} catch (RuntimeException t) {

				log.error(t,t);
				
			}

		}

    }

	/**
	 * Log the query.
	 * 
	 * @param q
	 *            The query.
	 * @param sb
	 *            Where to write the log message.
	 */
	static public void log(final boolean includeTableHeader,
			final IRunningQuery q, final StringBuilder sb) {

		if(includeTableHeader) {
			
			sb.append(getTableHeader());
			
		}
		
		logDetailRows(q, sb);

    	logSummaryRow(q, sb);
    	
    }
    
	/**
	 * Log a detail row for each operator in the query.
	 */
	static private void logDetailRows(final IRunningQuery q,
			final StringBuilder sb) {

		final Integer[] order = BOpUtility.getEvaluationOrder(q.getQuery());

		int orderIndex = 0;
		
		for (Integer bopId : order) {

			sb.append(getTableRow(q, orderIndex, bopId, false/* summary */));
			
//			sb.append('\n');
			
			orderIndex++;
			
		}

	}

    /**
     * Log a summary row for the query.
     */
    static private void logSummaryRow(final IRunningQuery q, final StringBuilder sb) {

		sb.append(getTableRow(q, -1/* orderIndex */, q.getQuery().getId(), true/* summary */));
		
//		sb.append('\n');

    }
    
    static private String getTableHeader() {

        final StringBuilder sb = new StringBuilder();

        /*
         * Common columns for the overall query and for each pipeline operator.
         */
        sb.append("queryId");
        sb.append("\ttag");
        sb.append("\tbeginTime");
        sb.append("\tdoneTime");
        sb.append("\tdeadline");
        sb.append("\telapsed");
        sb.append("\tserviceId");
        sb.append("\tcause");
        sb.append("\tbop");
        /*
         * Columns for each pipeline operator.
         */
        sb.append("\tevalOrder"); // [0..n-1]
        sb.append("\tbopId");
        sb.append("\tpredId");
        sb.append("\tevalContext");
        sb.append("\tcontroller");
        // metadata considered by the static optimizer.
        sb.append("\tstaticBestKeyOrder"); // original key order assigned by static optimizer.
        sb.append("\tnvars"); // #of variables in the predicate for a join.
        sb.append("\tfastRangeCount"); // fast range count used by the static optimizer.
        // dynamics (aggregated for totals as well).
        sb.append("\tfanIO");
        sb.append("\tsumMillis"); // cumulative milliseconds for eval of this operator.
        sb.append("\topCount"); // cumulative #of invocations of tasks for this operator.
        sb.append("\tchunksIn");
        sb.append("\tunitsIn");
        sb.append("\tchunksOut");
        sb.append("\tunitsOut");
        sb.append("\tjoinRatio"); // expansion rate multipler in the solution count.
        sb.append("\taccessPathDups");
        sb.append("\taccessPathCount");
        sb.append("\taccessPathRangeCount");
        sb.append("\taccessPathChunksIn");
        sb.append("\taccessPathUnitsIn");
        // dynamics based on elapsed wall clock time.
        sb.append("\tsolutions/ms");
        sb.append("\tmutations/ms");
        //
        // cost model(s)
        //
        sb.append('\n');

        return sb.toString();

    }

    /**
     * Return a tabular representation of the query {@link RunState}.
     *
     * @param q The {@link IRunningQuery}.
     * @param evalOrder The evaluation order for the operator.
     * @param bopId The identifier for the operator.
     * @param summary <code>true</code> iff the summary for the query should be written.
     * @return The row of the table.
     */
	static private String getTableRow(final IRunningQuery q,
			final int evalOrder, final Integer bopId, final boolean summary) {

        final StringBuilder sb = new StringBuilder();

        final DateFormat dateFormat = DateFormat.getDateTimeInstance(
                DateFormat.FULL, DateFormat.FULL);
        
        // The elapsed time for the query (wall time in milliseconds).
        final long elapsed = q.getElapsed();
        
        // The serviceId on which the query is running : null unless scale-out.
        final UUID serviceId = q.getQueryEngine().getServiceUUID();
        
        // The thrown cause : null unless the query was terminated abnormally.
        final Throwable cause = q.getCause();
        
        sb.append(q.getQueryId());
        sb.append('\t');
		sb.append(q.getQuery().getProperty(QueryHints.TAG,
				QueryHints.DEFAULT_TAG));
		sb.append('\t');
        sb.append(dateFormat.format(new Date(q.getStartTime())));
        sb.append('\t');
        sb.append(dateFormat.format(new Date(q.getDoneTime())));
        sb.append('\t');
        if(q.getDeadline()!=Long.MAX_VALUE)
        	sb.append(dateFormat.format(new Date(q.getDeadline())));
        sb.append('\t');
        sb.append(elapsed);
        sb.append('\t');
        sb.append(serviceId == null ? NA : serviceId.toString());
        sb.append('\t');
        if (cause != null) 
            sb.append(cause.getLocalizedMessage());

    	final Map<Integer, BOp> bopIndex = q.getBOpIndex();
    	final Map<Integer, BOpStats> statsMap = q.getStats();
		final BOp bop = bopIndex.get(bopId);

		// the operator.
		sb.append('\t');
		if (summary) {
			/*
			 * The entire query (recursively). New lines are translated out to
			 * keep this from breaking the table format.
			 */
			sb.append(BOpUtility.toString(q.getQuery()).replace('\n', ' '));
            sb.append('\t');
            sb.append("total"); // summary line.
        } else {
        	// Otherwise show just this bop.
        	sb.append(bopIndex.get(bopId).toString());
	        sb.append('\t');
	        sb.append(evalOrder); // eval order for this bop.
        }
        
        sb.append('\t');
        sb.append(Integer.toString(bopId));
		sb.append('\t');
		{
			/*
			 * Show the predicate identifier if this is a Join operator.
			 * 
			 * @todo handle other kinds of join operators when added using a
			 * shared interface.
			 */
			final IPredicate<?> pred = (IPredicate<?>) bop
					.getProperty(PipelineJoin.Annotations.PREDICATE);
			if (pred != null) {
				try {
					final int predId = pred.getId();
					sb.append(Integer.toString(predId));
				} catch (IllegalStateException ex) {
					/*
					 * All predicates SHOULD have a bopId, but this catches the
					 * error if one does not.
					 */
					sb.append(NA);
				}
			}
		}
		sb.append('\t');
		sb.append(bop.getEvaluationContext());
		sb.append('\t');
		sb.append(bop.getProperty(BOp.Annotations.CONTROLLER,
				BOp.Annotations.DEFAULT_CONTROLLER));

		/*
		 * Static optimizer metadata.
		 * 
		 * FIXME Should report [nvars] be the expected asBound #of variables
		 * given the assigned evaluation order and the expectation of propagated
		 * bindings (optionals may leave some unbound).
		 */
		{

			@SuppressWarnings("unchecked")
			final IPredicate pred = (IPredicate<?>) bop
					.getProperty(PipelineJoin.Annotations.PREDICATE);
			
			if (pred != null) {
			
				final IKeyOrder<?> keyOrder = (IKeyOrder<?>) pred
						.getProperty(Rule2BOpUtility.Annotations.ORIGINAL_INDEX);
				
				final Long rangeCount = (Long) pred
						.getProperty(Rule2BOpUtility.Annotations.ESTIMATED_CARDINALITY);
				
				sb.append('\t'); // keyorder
				if (keyOrder != null)
					sb.append(keyOrder);
				
				sb.append('\t'); // nvars
				if (keyOrder != null)
					sb.append(pred.getVariableCount(keyOrder));
				
				sb.append('\t'); // rangeCount
				if (rangeCount!= null)
					sb.append(rangeCount);
				
			} else {
				sb.append('\t'); // keyorder
				sb.append('\t'); // nvars
				sb.append('\t'); // rangeCount
			}
		}

		/*
		 * Dynamics.
		 */
		
		int fanIO = 0; // @todo aggregate from RunState.

		final PipelineJoinStats stats = new PipelineJoinStats();
		if(summary) {
	    	// Aggregate the statistics for all pipeline operators.
			for (BOpStats t : statsMap.values()) {
				stats.add(t);
			}
		} else {
            // Just this operator.
            final BOpStats tmp = statsMap.get(bopId);
            if (tmp != null)
                stats.add(tmp);
		}
		final long unitsIn = stats.unitsIn.get();
		final long unitsOut = stats.unitsOut.get();
		sb.append('\t');
		sb.append(Integer.toString(fanIO));
		sb.append('\t');
		sb.append(stats.elapsed.get());
		sb.append('\t');
		sb.append(stats.opCount.get());
		sb.append('\t');
		sb.append(stats.chunksIn.get());
		sb.append('\t');
		sb.append(stats.unitsIn.get());
		sb.append('\t');
		sb.append(stats.chunksOut.get());
		sb.append('\t');
		sb.append(stats.unitsOut.get());
		sb.append('\t');
		sb.append(unitsIn == 0 ? NA : unitsOut / (double) unitsIn);
		sb.append('\t');
		sb.append(stats.accessPathDups.get());
		sb.append('\t');
		sb.append(stats.accessPathCount.get());
		sb.append('\t');
		sb.append(stats.accessPathRangeCount.get());
		sb.append('\t');
		sb.append(stats.accessPathChunksIn.get());
		sb.append('\t');
		sb.append(stats.accessPathUnitsIn.get());

		/*
		 * Use the total elapsed time for the query (wall time).
		 */
		// solutions/ms
		sb.append('\t');
		sb.append(elapsed == 0 ? 0 : stats.unitsOut.get() / elapsed);
		// mutations/ms : @todo mutations/ms.
		sb.append('\t');
//		sb.append(elapsed==0?0:stats.unitsOut.get()/elapsed);

        sb.append('\n');

        return sb.toString();

    }

	/**
	 * Format the data as an (X)HTML table. The table will include a header
	 * which declares the columns, a detail row for each operator (optional),
	 * and a summary row for the query as a whole.
	 * 
	 * @param queryStr
	 *            The original text of the query (e.g., a SPARQL query)
	 *            (optional).
	 * @param q
	 *            The {@link IRunningQuery}.
	 * @param w
	 *            Where to write the table.
	 * @param summaryOnly
	 *            When <code>true</code> only the summary row will be written.
	 * @param maxBopLength
	 *            The maximum length to display from {@link BOp#toString()} and
	 *            ZERO (0) to display everything.  Data longer than this value
	 *            will be accessible from a flyover, but not directly visible
	 *            in the page.
	 * @throws IOException
	 */
	public static void getTableXHTML(final String queryStr,
			final IRunningQuery q, final Writer w, final boolean summaryOnly,
			final int maxBopLength)
			throws IOException {

		// the table start tag.
		w.write("<table border=\"1\" summary=\"" + attrib("Query Statistics")
				+ "\"\n>");
        
        getTableHeaderXHTML(q, w);

    	if(summaryOnly) {

    		getSummaryRowXHTML(queryStr, q, w, maxBopLength);
    		
    	} else {

    		// Summary first.
    		getSummaryRowXHTML(queryStr, q, w, maxBopLength);

    		// Then the detail rows.
    		getTableRowsXHTML(queryStr, q, w, maxBopLength);
    		
    	}

    	w.write("</table\n>");
    	
	}
	
	public static void getTableHeaderXHTML(final IRunningQuery q, final Writer w)
			throws IOException {

        // header row.
        w.write("<tr\n>");
        /*
         * Common columns for the overall query and for each pipeline operator.
         */
        w.write("<th>queryId</th>");
        w.write("<th>tag</th>");
        w.write("<th>beginTime</th>");
        w.write("<th>doneTime</th>");
        w.write("<th>deadline</th>");
        w.write("<th>elapsed</th>");
        w.write("<th>serviceId</th>");
        w.write("<th>cause</th>");
//        w.write("<th>query</th>");
//        w.write("<th>bop</th>");
        /*
         * Columns for each pipeline operator.
         */
        w.write("<th>evalOrder</th>"); // [0..n-1]
        w.write("<th>bopId</th>");
        w.write("<th>predId</th>");
        w.write("<th>evalContext</th>");
        w.write("<th>controller</th>");
        // metadata considered by the static optimizer.
        w.write("<th>staticBestKeyOrder</th>"); // original key order assigned
                                                // by static optimizer.
        w.write("<th>nvars</th>"); // #of variables in the predicate for a join.
        w.write("<th>fastRangeCount</th>"); // fast range count used by the
                                            // static optimizer.
        // dynamics (aggregated for totals as well).
        w.write("<th>fanIO</th>");
        w.write("<th>sumMillis</th>"); // cumulative milliseconds for eval of
                                       // this operator.
        w.write("<th>opCount</th>"); // cumulative #of invocations of tasks for
                                     // this operator.
        w.write("<th>chunksIn</th>");
        w.write("<th>unitsIn</th>");
        w.write("<th>chunksOut</th>");
        w.write("<th>unitsOut</th>");
        w.write("<th>joinRatio</th>"); // expansion rate multiplier in the
                                       // solution count.
        w.write("<th>accessPathDups</th>");
        w.write("<th>accessPathCount</th>");
        w.write("<th>accessPathRangeCount</th>");
        w.write("<th>accessPathChunksIn</th>");
        w.write("<th>accessPathUnitsIn</th>");
        // dynamics based on elapsed wall clock time.
        w.write("<th>");w.write(cdata("solutions/ms"));w.write("</th>");
        w.write("<th>");w.write(cdata("mutations/ms"));w.write("</th>");
        //
        // cost model(s)
        //
        w.write("</tr\n>");

    }

	/**
	 * Write the table rows.
	 * 
	 * @param queryStr
	 *            The query text (optional).
	 * @param q
	 *            The {@link IRunningQuery}.
	 * @param w
	 *            Where to write the rows.
	 * @param maxBopLength
	 *            The maximum length to display from {@link BOp#toString()} and
	 *            ZERO (0) to display everything. Data longer than this value
	 *            will be accessible from a flyover, but not directly visible in
	 *            the page.
	 * 
	 * @throws IOException
	 */
	public static void getTableRowsXHTML(final String queryStr,
			final IRunningQuery q, final Writer w, final int maxBopLength)
			throws IOException {

        final Integer[] order = BOpUtility.getEvaluationOrder(q.getQuery());

        int orderIndex = 0;
        
        for (Integer bopId : order) {

			getTableRowXHTML(queryStr, q, w, orderIndex, bopId,
					false/* summary */, maxBopLength);

            orderIndex++;
            
        }

    }

	/**
	 * Return a tabular representation of the query {@link RunState}.
	 * 
	 * @param queryStr
	 *            The query text (optional).
	 * @param q
	 *            The {@link IRunningQuery}.
	 * @param evalOrder
	 *            The evaluation order for the operator.
	 * @param bopId
	 *            The identifier for the operator.
	 * @param summary
	 *            <code>true</code> iff the summary for the query should be
	 *            written.
	 * @param maxBopLength
	 *            The maximum length to display from {@link BOp#toString()} and
	 *            ZERO (0) to display everything.  Data longer than this value
	 *            will be accessible from a flyover, but not directly visible
	 *            in the page.
	 *            
	 * @return The row of the table.
	 */
	static private void getTableRowXHTML(final String queryStr,
			final IRunningQuery q, final Writer w, final int evalOrder,
			final Integer bopId, final boolean summary, final int maxBopLength)
			throws IOException {

        final DateFormat dateFormat = DateFormat.getDateTimeInstance(
                DateFormat.FULL, DateFormat.FULL);
        
        // The elapsed time for the query (wall time in milliseconds).
        final long elapsed = q.getElapsed();
        
        // The serviceId on which the query is running : null unless scale-out.
        final UUID serviceId = q.getQueryEngine().getServiceUUID();
        
        // The thrown cause : null unless the query was terminated abnormally.
        final Throwable cause = q.getCause();
        
        w.write("<tr\n>");
        w.write(TD + cdata(q.getQueryId().toString()) + TDx);
        w.write(TD
                + cdata(q.getQuery().getProperty(QueryHints.TAG,
                        QueryHints.DEFAULT_TAG)) + TDx);
        w.write(TD + dateFormat.format(new Date(q.getStartTime())) + TDx);
        w.write(TD + cdata(dateFormat.format(new Date(q.getDoneTime()))) + TDx);
        w.write(TD);
        if (q.getDeadline() != Long.MAX_VALUE)
            w.write(cdata(dateFormat.format(new Date(q.getDeadline()))));
        w.write(TDx);
        w.write(TD + cdata(Long.toString(elapsed)) + TDx);
        w.write(TD); w.write(cdata(serviceId == null ? NA : serviceId.toString()));w.write(TDx);
        w.write(TD);
        if (cause != null)
            w.write(cause.getLocalizedMessage());
        w.write(TDx);
        
        final Map<Integer, BOp> bopIndex = q.getBOpIndex();
        final Map<Integer, BOpStats> statsMap = q.getStats();
        final BOp bop = bopIndex.get(bopId);

        // the operator.
        if (summary) {
//        	// The query string (SPARQL).
//            w.write(TD);
//			w.write(queryStr == null ? cdata(NA) : prettyPrintSparql(queryStr));
//            w.write(TDx);
//            // The query plan (BOPs)
//        	{
//				w.write(TD);
//				final String bopStr = BOpUtility.toString(q.getQuery());
//				if (maxBopLength == 0 || bopStr.length() <= maxBopLength) {
//					// The entire query plan.
//					w.write(cdata(bopStr));
//				} else {
//					// A slice of the query plan.
//					w.write("<a href=\"#\" title=\"");
//					w.write(attrib(bopStr));// the entire query as a tooltip.
//					w.write("\"\n>");
//					w.write(cdata(bopStr.substring(0/* begin */, Math.min(
//							maxBopLength, bopStr.length()))));
//					w.write("...");
//					w.write("</a>");
//				}
//				w.write(TDx);
//        	}
            w.write(TD);
            w.write("total"); // summary line.
            w.write(TDx);
        } else {
//        	// The query string (SPARQL).
//            w.write(TD);
//            w.write("...");// elide the original query string on a detail row.
//            w.write(TDx);
//			// The query plan (BOPs)
//			{
//				w.write(TD);
//				final String bopStr = bopIndex.get(bopId).toString();
//				if (maxBopLength == 0 || bopStr.length() <= maxBopLength) {
//					// The entire query plan.
//					w.write(cdata(bopStr));
//				} else {
//					// A slice of the query plan.
//					w.write("<a href=\"#\" title=\"");
//					w.write(attrib(bopStr));// the entire query as a tooltip.
//					w.write("\"\n>");
//					// A slice of the query inline on the page.
//					w.write(cdata(bopStr.substring(0/* begin */, Math.min(
//							maxBopLength, bopStr.length()))));
//					w.write("...");
//					w.write("</a>");
//				}
//				w.write(TDx);
//			}
            w.write(TD);
            w.write(Integer.toString(evalOrder)); // eval order for this bop.
            w.write(TDx);
        }
        
        w.write(TD);
        w.write(Integer.toString(bopId));
        w.write(TDx);
        {
            /*
             * Show the predicate identifier if this is a Join operator.
             * 
             * @todo handle other kinds of join operators when added using a
             * shared interface.
             */
            final IPredicate<?> pred = (IPredicate<?>) bop
                    .getProperty(PipelineJoin.Annotations.PREDICATE);
            w.write(TD);
            if (pred != null) {
				try {
					final int predId = pred.getId();
					w.write(Integer.toString(predId));
				} catch (IllegalStateException ex) {
					/*
					 * All predicates SHOULD have a bopId, but this catches the
					 * error if one does not.
					 */
					w.write(cdata(NA));
				}
            }
            w.write(TDx);
        }
        w.write(TD);
        w.write(cdata(bop.getEvaluationContext().toString()));
        w.write(TDx);
        w.write(TD);
        w.write(cdata(bop.getProperty(BOp.Annotations.CONTROLLER,
                BOp.Annotations.DEFAULT_CONTROLLER).toString()));
        w.write(TDx);

        /*
         * Static optimizer metadata.
         * 
         * FIXME Should report [nvars] be the expected asBound #of variables
         * given the assigned evaluation order and the expectation of propagated
         * bindings (optionals may leave some unbound).
         */
        {

            @SuppressWarnings("unchecked")
            final IPredicate pred = (IPredicate<?>) bop
                    .getProperty(PipelineJoin.Annotations.PREDICATE);
            
            if (pred != null) {
            
                final IKeyOrder<?> keyOrder = (IKeyOrder<?>) pred
                        .getProperty(Rule2BOpUtility.Annotations.ORIGINAL_INDEX);
                
                final Long rangeCount = (Long) pred
                        .getProperty(Rule2BOpUtility.Annotations.ESTIMATED_CARDINALITY);

                // keyorder
                w.write(TD);
                if (keyOrder != null)
                    w.write(keyOrder.toString());
                w.write(TDx);

                // nvars
                w.write(TD);
                if (keyOrder != null)
                    w.write(Integer.toString(pred.getVariableCount(keyOrder)));
                w.write(TDx);

                // rangeCount
                w.write(TD);
                if (rangeCount != null)
                    w.write(Long.toString(rangeCount));
                w.write(TDx);

            } else {
                // keyorder
                w.write(TD);
                w.write(TDx);
                // nvars
                w.write(TD);
                w.write(TDx);
                // rangeCount
                w.write(TD);
                w.write(TDx);
            }
        }

        /*
         * Dynamics.
         */
        
        int fanIO = 0; // @todo aggregate from RunState.

        final PipelineJoinStats stats = new PipelineJoinStats();
        if(summary) {
            // Aggregate the statistics for all pipeline operators.
            for (BOpStats t : statsMap.values()) {
                stats.add(t);
            }
        } else {
            // Just this operator.
            final BOpStats tmp = statsMap.get(bopId);
            if (tmp != null)
                stats.add(tmp);
        }
        final long unitsIn = stats.unitsIn.get();
        final long unitsOut = stats.unitsOut.get();
        w.write(TD);
        w.write(Integer.toString(fanIO));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.elapsed.get()));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.opCount.get()));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.chunksIn.get()));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.unitsIn.get()));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.chunksOut.get()));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.unitsOut.get()));
        w.write(TDx);
        w.write(TD);
        w.write(cdata(unitsIn == 0 ? NA : Double.toString(unitsOut / (double) unitsIn)));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.accessPathDups.get()));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.accessPathCount.get()));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.accessPathRangeCount.get()));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.accessPathChunksIn.get()));
        w.write(TDx);
        w.write(TD);
        w.write(Long.toString(stats.accessPathUnitsIn.get()));
        w.write(TDx);

        /*
         * Use the total elapsed time for the query (wall time).
         */
        // solutions/ms
        w.write(TD);
        w.write(cdata(elapsed == 0 ? "0" : Long.toString(stats.unitsOut.get()
                / elapsed)));
        w.write(TDx);
        // mutations/ms : @todo mutations/ms.
        w.write(TD);
        w.write(TDx);
//      w.write(elapsed==0?0:stats.unitsOut.get()/elapsed);

        w.write("</tr\n>");

    }

    /**
     * Write a summary row for the query.  The table element, header, and footer
     * must be written separately.
     * @param queryStr The original query text (optional).
     * @param q The {@link IRunningQuery}.
     * @param w Where to write the data.
	 * @param maxBopLength
	 *            The maximum length to display from {@link BOp#toString()} and
	 *            ZERO (0) to display everything.  Data longer than this value
	 *            will be accessible from a flyover, but not directly visible
	 *            in the page.
     * @throws IOException
     */
	static public void getSummaryRowXHTML(final String queryStr,
			final IRunningQuery q, final Writer w, final int maxBopLength)
			throws IOException {

		getTableRowXHTML(queryStr, q, w, -1/* orderIndex */, q.getQuery()
				.getId(), true/* summary */, maxBopLength);

    }

    private static String cdata(String s) {
        
        return XHTMLRenderer.cdata(s);
        
    }

    private static String attrib(String s) {
        
        return XHTMLRenderer.attrib(s);
        
    }

    private static String prettyPrintSparql(String s) {

//    	return cdata(s);
//    	
//    }
    
    	s = s.replace("\n", " ");
    	
    	s = s.replace("PREFIX", "\nPREFIX");
    	s = s.replace("select", "\nselect");
    	s = s.replace("where", "\nwhere");
    	s = s.replace("{","{\n");
    	s = s.replace("}","\n}");
    	s = s.replace(" ."," .\n"); // TODO Must not match within quotes (literals) or <> (URIs).
//    	s = s.replace("||","||\n");
//    	s = s.replace("&&","&&\n");
    	
    	s = cdata(s);
    	
    	s = s.replace("\n", "<br>");
    	
//    	return "<pre>"+s+"</pre>";
    	
    	return s;
    	
    }
    
}
