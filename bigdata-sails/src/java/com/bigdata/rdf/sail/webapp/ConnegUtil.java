/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
 * Created on Mar 6, 2012
 */

package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultWriter;
import org.openrdf.query.resultio.BooleanQueryResultWriterFactory;
import org.openrdf.query.resultio.BooleanQueryResultWriterRegistry;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.rio.RDFFormat;

import com.bigdata.counters.format.CounterSetFormat;
import com.bigdata.rdf.properties.PropertiesFormat;
import com.bigdata.rdf.sail.webapp.client.MiniMime;

/**
 * Helper class for content negotiation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ConnegUtil {
	
    private static Logger log = Logger.getLogger(ConnegUtil.class);

    private static final Pattern pattern;
    static {
        pattern = Pattern.compile("\\s*,\\s*");
    }


	static {
	// Work-around for sesame not handling ask and json (see trac 704 and 714)
		
		if (BooleanQueryResultFormat.forMIMEType(BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON)!=null) {
			// This should fire once trac 714 is fixed, and we have upgraded, at this point the whole static block should be deleted.
			log.warn("Workaround for sesame 2.6 BooleanQueryResultFormat defect no longer needed", new RuntimeException("location of issue"));
		} else {
			final BooleanQueryResultFormat askJsonFormat = BooleanQueryResultFormat.register("SPARQL/JSON",BigdataRDFServlet.MIME_SPARQL_RESULTS_JSON,"srj");
			BooleanQueryResultWriterRegistry.getInstance().add(new BooleanQueryResultWriterFactory(){

				@Override
				public BooleanQueryResultFormat getBooleanQueryResultFormat() {
					return askJsonFormat;
				}

				@Override
				public BooleanQueryResultWriter getWriter(final OutputStream out) {
					return new BooleanQueryResultWriter(){

						@Override
						public BooleanQueryResultFormat getBooleanQueryResultFormat() {
							return askJsonFormat;
						}

						@Override
						public void write(boolean arg0) throws IOException {
							final String answer = "{ \"head\":{ } , \"boolean\": " +  Boolean.toString(arg0) + " }";
							out.write(answer.getBytes("utf-8"));
						}};
				}});
		}
	}
	
    private final ConnegScore<?>[] scores;

    /**
     * 
     * @param acceptStr
     *            The <code>Accept</code> header.
     */
    public ConnegUtil(String acceptStr) {

        if (acceptStr == null) {
            /*
             * If no Accept header is present, then the client has no
             * preferences.
             */
//            throw new IllegalArgumentException();
            acceptStr = "";
        }

        final String[] a = pattern.split(acceptStr);

        final List<ConnegScore<?>> scores = new LinkedList<ConnegScore<?>>();

        {

            for (String s : a) {

                final MiniMime t = new MiniMime(s);

                // RDFFormat
                {

                    final RDFFormat rdfFormat = RDFFormat
                            .forMIMEType(t.getMimeType());

                    if (rdfFormat != null) {

                        scores.add(new ConnegScore<RDFFormat>(t.q, rdfFormat));

                    }

                }
                
                // TupleQueryResultFormat
                {
                
                    final TupleQueryResultFormat tupleFormat = TupleQueryResultFormat
                            .forMIMEType(t.getMimeType());

                    if (tupleFormat != null) {

                        scores.add(new ConnegScore<TupleQueryResultFormat>(t.q,
                                tupleFormat));
                    }
                    
                }

                // BooleanQueryResultFormat
                {
                
                    BooleanQueryResultFormat booleanFormat = BooleanQueryResultFormat
                            .forMIMEType(t.getMimeType());

                    if (booleanFormat != null) {

                        scores.add(new ConnegScore<BooleanQueryResultFormat>(
                                t.q, booleanFormat));

                    }

                }

                // PropertiesFormat
                {

                    final PropertiesFormat format = PropertiesFormat
                            .forMIMEType(t.getMimeType());

                    if (format != null) {

                        scores.add(new ConnegScore<PropertiesFormat>(t.q,
                                format));

                    }

                }
                
                // CounterSetFormat
                {

                    final CounterSetFormat format = CounterSetFormat
                            .forMIMEType(t.getMimeType());

                    if (format != null) {

                        scores.add(new ConnegScore<CounterSetFormat>(t.q,
                                format));

                    }

                }
                
            }

        }

        this.scores = scores.toArray(new ConnegScore[scores.size()]);

        // Order by quality.
        Arrays.sort(this.scores);

    }

    /**
     * Return the best {@link RDFFormat} from the <code>Accept</code> header,
     * where "best" is measured by the <code>q</code> parameter.
     * 
     * @return The best {@link RDFFormat} -or- <code>null</code> if no
     *         {@link RDFFormat} was requested.
     */
    public RDFFormat getRDFFormat() {
        
        return getRDFFormat(null/*fallback*/);
        
    }
    
    /**
     * Return the best {@link RDFFormat} from the <code>Accept</code> header,
     * where "best" is measured by the <code>q</code> parameter.
     * 
     * @param fallback
     *            The caller's default, which is returned if no match was
     *            specified.
     * 
     * @return The best {@link RDFFormat} -or- <i>fallback</i> if no
     *         {@link RDFFormat} was requested.
     */
    public RDFFormat getRDFFormat(final RDFFormat fallback) {

        for (ConnegScore<?> s : scores) {

            if (s.format instanceof RDFFormat) {

                return (RDFFormat) s.format;

            }

        }

        return fallback;

    }
    
    /**
     * Return the best {@link TupleQueryResultFormat} from the
     * <code>Accept</code> header, where "best" is measured by the
     * <code>q</code> parameter.
     * 
     * @return The best {@link TupleQueryResultFormat} -or- <code>null</code> if
     *         no {@link TupleQueryResultFormat} was requested.
     */
    public TupleQueryResultFormat getTupleQueryResultFormat() {
        
        return getTupleQueryResultFormat(null/*fallback*/);
        
    }

    /**
     * Return the best {@link TupleQueryResultFormat} from the
     * <code>Accept</code> header, where "best" is measured by the
     * <code>q</code> parameter.
     * 
     * @param fallback
     *            The caller's default, which is returned if no match was
     *            specified.
     * 
     * @return The best {@link TupleQueryResultFormat} -or- <i>fallback</i> if
     *         no {@link TupleQueryResultFormat} was requested.
     */
    public TupleQueryResultFormat getTupleQueryResultFormat(
            final TupleQueryResultFormat fallback) {

        for (ConnegScore<?> s : scores) {

            if (s.format instanceof TupleQueryResultFormat) {

                return (TupleQueryResultFormat) s.format;

            }

        }

        return fallback;

    }

    /**
     * Return the best {@link BooleanQueryResultFormat} from the
     * <code>Accept</code> header, where "best" is measured by the
     * <code>q</code> parameter.
     * 
     * @return The best {@link BooleanQueryResultFormat} -or- <code>null</code>
     *         if no {@link BooleanQueryResultFormat} was requested.
     */
    public BooleanQueryResultFormat getBooleanQueryResultFormat() {
        
        return getBooleanQueryResultFormat(null/* fallback */);
        
    }

    /**
     * Return the best {@link BooleanQueryResultFormat} from the
     * <code>Accept</code> header, where "best" is measured by the
     * <code>q</code> parameter.
     * 
     * @param fallback
     *            The caller's default, which is returned if no match was
     *            specified.
     *            
     * @return The best {@link BooleanQueryResultFormat} -or- <i>fallback</i> if
     *         no {@link BooleanQueryResultFormat} was requested.
     */
    public BooleanQueryResultFormat getBooleanQueryResultFormat(
            final BooleanQueryResultFormat fallback) {

        for (ConnegScore<?> s : scores) {

            if (s.format instanceof BooleanQueryResultFormat) {

                return (BooleanQueryResultFormat) s.format;

            }

        }

        return fallback;

    }

    /**
     * Return the best {@link PropertiesFormat} from the <code>Accept</code>
     * header, where "best" is measured by the <code>q</code> parameter.
     * 
     * @return The best {@link PropertiesFormat} -or- <code>null</code> if no
     *         {@link PropertiesFormat} was requested.
     */
    public PropertiesFormat getPropertiesFormat() {

        return getPropertiesFormat(null/* fallback */);

    }

    /**
     * Return the best {@link PropertiesFormat} from the <code>Accept</code>
     * header, where "best" is measured by the <code>q</code> parameter.
     * 
     * @param fallback
     *            The caller's default, which is returned if no match was
     *            specified.
     * 
     * @return The best {@link PropertiesFormat} -or- <i>fallback</i> if no
     *         {@link PropertiesFormat} was requested.
     */
    public PropertiesFormat getPropertiesFormat(final PropertiesFormat fallback) {

        for (ConnegScore<?> s : scores) {

            if (s.format instanceof PropertiesFormat) {

                return (PropertiesFormat) s.format;

            }

        }

        return fallback;

    }

    /**
     * Return the best {@link CounterSetFormat} from the <code>Accept</code>
     * header, where "best" is measured by the <code>q</code> parameter.
     * 
     * @return The best {@link CounterSetFormat} -or- <code>null</code> if no
     *         {@link CounterSetFormat} was requested.
     */
    public CounterSetFormat getCounterSetFormat() {

        return getCounterSetFormat(null/* fallback */);

    }

    /**
     * Return the best {@link RDCountersFormatFFormat} from the
     * <code>Accept</code> header, where "best" is measured by the
     * <code>q</code> parameter.
     * 
     * @param fallback
     *            The caller's default, which is returned if no match was
     *            specified.
     * 
     * @return The best {@link CounterSetFormat} -or- <i>fallback</i> if no
     *         {@link CounterSetFormat} was requested.
     */
    public CounterSetFormat getCounterSetFormat(final CounterSetFormat fallback) {

        for (ConnegScore<?> s : scores) {

            if (s.format instanceof CounterSetFormat) {

                return (CounterSetFormat) s.format;

            }

        }

        return fallback;

    }
    
    /**
     * Return an ordered list of the {@link ConnegScore}s for MIME Types which
     * are consistent with the desired format type.
     * 
     * @param cls
     *            The format type.
     * 
     * @return The ordered list.
     */
    @SuppressWarnings("unchecked")
    public <E> ConnegScore<E>[] getScores(final Class<E> cls) {

        if (cls == null)
            throw new IllegalArgumentException();

        final List<ConnegScore<E>> t = new LinkedList<ConnegScore<E>>();

        for (ConnegScore<?> s : scores) {

            if (cls == s.format.getClass()) {

                t.add((ConnegScore<E>) s);

            }

        }

        return t.toArray(new ConnegScore[t.size()]);

    }

    @Override
    public String toString() {

        return getClass().getSimpleName() + "{" + Arrays.toString(scores) + "}";

    }

}
