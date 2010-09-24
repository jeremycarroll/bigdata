/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Aug 13, 2007
 */

package com.bigdata.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import junit.framework.TestCase2;

import com.bigdata.util.CSVReader.Header;

/**
 * Test suite for {@link CSVReader}.
 * 
 * @todo test "correct" default intepretation of more kinds of formats by
 *       {@link Header#parseValue(String)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCSVReader extends TestCase2 {

    /**
     * 
     */
    public TestCSVReader() {
        super();
    }

    /**
     * @param name
     */
    public TestCSVReader(String name) {
        super(name);
    }

    public void test_ctor1_correctRejection() 
        throws IOException, SecurityException, NoSuchMethodException, 
               InstantiationException, IllegalAccessException, InvocationTargetException 
    {
        Object[][] cmdLines = {
            new Object[] {null, null},
            new Object[] {null, "UTF-8"},
            new Object[] {new ByteArrayInputStream(new byte[]{}), null},            
        };
        
        Constructor<CSVReader> cons = 
        	CSVReader.class.getConstructor(InputStream.class, String.class);
        for (Object[] cmdLine: cmdLines) {
	        try {
	            cons.newInstance(cmdLine);
	            fail("Expecting: " + IllegalArgumentException.class.toString());
	        } catch(InvocationTargetException ex) {
	            if (!(ex.getCause() instanceof IllegalArgumentException)) {
		            fail("Expecting: " + IllegalArgumentException.class.toString());
	            }
	            //ignore -- expected
	        }
        }

    }
    
    public void test_ctor2_correctRejection() throws IOException {
        try {
        	Reader r = null;
        	new CSVReader(r);
            fail("Expecting: " + IllegalArgumentException.class.toString());
        } catch(IllegalArgumentException ex) {
            //ignore -- expected
        }
	}
    
    /**
     * Test reads from a tab-delimited file <code>test.csv</code> with headers
     * in the first row and two rows of data.
     * 
     * @throws IOException
     * @throws ParseException
     */
    public void test_read_test_csv() throws IOException, ParseException {
        
        Header[] headers = new Header[] {
          
                new Header("Name"),
                new Header("Id"),
                new Header("Employer"),
                new Header("DateOfHire"),
                new Header("Salary"),
                
        };
        
        CSVReader r = new CSVReader(
                getTestInputStream("com/bigdata/util/test.csv"), "UTF-8");

        /* 
         * read headers.
         */
        assertTrue(r.hasNext());

        r.readHeaders();
        
        assertEquals(1,r.lineNo());

        assertEquals(headers, r.getHeaders());
        
        /*
         * 1st row of data.
         */
        assertTrue(r.hasNext());

        assertSameValues(newMap(headers, new Object[] { "Bryan Thompson",
                new Long(12), "SAIC",
                new SimpleDateFormat("MM/dd/yy").parse("4/30/2002"),
                new Double(12.02)
        }), r.next() );

        assertEquals(2,r.lineNo());

        
        /*
         * 2nd row of data.
         */
        assertTrue(r.hasNext());

        assertSameValues( newMap(headers, new Object[]{
                "Bryan Thompson",
                new Long(12), "SYSTAP",
                new SimpleDateFormat("MM/dd/yy").parse("4/30/2005"),
                new Double(13.03)
        }), r.next() );

        assertEquals(3,r.lineNo());

        /* 
         * Verify EOF.
         */
        assertFalse(r.hasNext());
        
    }

    /**
     * Test reads from a tab-delimited file <code>test-no-headers.csv</code>
     * with NO headers and two rows of data.
     * 
     * @throws IOException
     * @throws ParseException
     */
    public void test_read_test_no_headers_csv() throws IOException, ParseException {
        
        Header[] headers = new Header[] {
          
                new Header("1"),
                new Header("2"),
                new Header("3"),
                new Header("4"),
                new Header("5"),
                
        };
        
        CSVReader r = new CSVReader(
                getTestInputStream("com/bigdata/util/test-no-headers.csv"), "UTF-8");

        /*
         * 1st row of data.
         */
        assertTrue(r.hasNext());

        assertSameValues(newMap(headers, new Object[] { "Bryan Thompson",
                new Long(12), "SAIC",
                new SimpleDateFormat("MM/dd/yy").parse("4/30/2002"),
                new Double(12.02)
        }), r.next() );

        assertEquals(1,r.lineNo());

        
        /*
         * 2nd row of data.
         */
        assertTrue(r.hasNext());

        assertSameValues( newMap(headers, new Object[]{
                "Bryan Thompson",
                new Long(12), "SYSTAP",
                new SimpleDateFormat("MM/dd/yy").parse("4/30/2005"),
                new Double(13.03)
        }), r.next() );

        assertEquals(2,r.lineNo());

        /* 
         * Verify EOF.
         */
        assertFalse(r.hasNext());
        
    }
    

    private static Header[] convertStringToHeader(String[] sa) {
    	Header[] h = new Header[sa.length];
    	int i=0;
    	for (String s: sa) {
    		h[i++] = new Header(s);
    	}
    	return h;
    }
    private static String[] stringHeaders = { "Header1", "Header2", "Header3" };
    private static String[] defaultStringHeaders = { "1", "2", "3" };
    
    private static Header[] headers = convertStringToHeader(stringHeaders);
    private static Header[] defaultHeaders = convertStringToHeader(defaultStringHeaders);
    
    private static Object[][] rows = {
    	{ "Column11", "Column12", "Column13" },
    	{ "Column21", "Column22", "Column23" },
        { "Column31", "Column32", "Column33" },
        { "Column with spaces", "and more spaces", "and embedded \"quotes\"" },
    };
    
    private static CSVReaderBuilder getDefaultTestCSVReaderBuilder() {
    	CSVReaderBuilder b = new CSVReaderBuilder();
    	for (String header: stringHeaders) {
    		b.header(header);
    	}
    	for (Object[] row: rows) {
    		b.newRow();
    		for (Object col: row) {
    			b.column(col.toString());
    		}
    	}
    	return b;
    }
    
    public void test_default_csv_reader_with_defaults() throws IOException {
    	CSVReaderBuilder cb = getDefaultTestCSVReaderBuilder();
    	verify_data_and_header(new CSVReader(cb.buildReader()));
    }
    public void test_default_csv_reader_with_tabs() throws IOException {
    	CSVReaderBuilder cb = getDefaultTestCSVReaderBuilder();
    	cb.setColumnDelimiter("\t");
    	verify_data_and_header(new CSVReader(cb.buildReader()));
    }
    public void test_default_csv_reader_without_quotes() throws IOException {
    	CSVReaderBuilder cb = getDefaultTestCSVReaderBuilder();
    	cb.setSuppressQoutes(true);
    	verify_data_and_header(new CSVReader(cb.buildReader()));
    }
    public void test_default_csv_reader_no_headers() throws IOException {
    	CSVReaderBuilder cb = getDefaultTestCSVReaderBuilder();
    	cb.setSuppressHeader(true);
    	verify_data(new CSVReader(cb.buildReader()),defaultHeaders);
    }
    
    private void verify_data_and_header(CSVReader cr) throws IOException {
        // Read and verify header row
        assertTrue(cr.hasNext());
        cr.readHeaders();
    	assertEquals(cr.getHeaders(), headers);
    	verify_data(cr, headers);
    }
    
    private void verify_data(CSVReader cr, Header[] headers) throws IOException {
    	//Read and verify data rows
    	for (int i=0; i < rows.length; i++) {
            assertTrue(cr.hasNext());
            assertSameValues( newMap(headers, rows[i]), cr.next() );
    	}    	
        assertFalse(cr.hasNext());
    }
    
    public void test_header_cons_with_bad_args() {
    	try {
    		new Header(null);
    		fail("Constructed Header with null arg.");
    	} catch (IllegalArgumentException e){
    		//ignore -- expected
    	}
    	try {
    		new Header("");
    		fail("Constructed Header with empty arg.");
    	} catch (IllegalArgumentException e){
    		//ignore -- expected
    	}    	
    }
    
    public void test_header_cons_with_good_arg() {
    	String name = "abc";
    	Header h = new Header(name);
    	assertEquals(h.getName(), name);
    }

    public void test_header_equals() {
    	String name = "abc";
    	Header h = new Header(name);
    	Header h_dup = new Header(name);
    	Header h_dup2 = new Header(name);
    	Header h_diff = new Header(name + "diff");
    	
    	// Test reflexive property
    	assertTrue(h.equals(h));
    	
    	// Test symmetric property    	
    	assertTrue(h.equals(h_dup) && h_dup.equals(h));
    	
    	//Test transitive property
    	assertTrue(h.equals(h_dup) && h_dup.equals(h_dup2) && h.equals(h_dup2));
    	
    	// consistency property already tested
    	
    	// Test negative cases
    	assertFalse(h.equals(null));
    	
    	assertFalse(h.equals(name));
    	
    	assertFalse(h.equals(h_diff));
    }
    
    public void test_header_hashcode() {
    	String name = "abc";
    	Header h = new Header(name);
    	Header h_dup = new Header(name);

    	assertTrue(h.hashCode()==h_dup.hashCode());
    }
    
    public void test_header_toString() {
    	String name = "abc";
    	Header h = new Header(name);
    	Header h_dup = new Header(name);

    	assertTrue(h.toString().equals(name));
    }
    
    public void test_setTailDelayMillis_bad_arg() throws IOException {
    	CSVReaderBuilder cb = getDefaultTestCSVReaderBuilder();
    	CSVReader r = new CSVReader(cb.buildReader());
    	try {
    		r.setTailDelayMillis(-1L);
    		fail("Created CSVReader with negative delay.");
    	} catch (IllegalArgumentException e) {
    		//ignore -- expected
    	}
    }
    
    public void test_setTailDelayMillis_good_arg() throws IOException {
    	CSVReaderBuilder cb = getDefaultTestCSVReaderBuilder();
    	CSVReader r = new CSVReader(cb.buildReader());
    	long delay = 1L;
    	long oldDelay = 0L;
    	long tmpDelay = 0L;
    	tmpDelay = r.setTailDelayMillis(delay);
  		assertTrue(r.getTailDelayMillis()==delay);
  		assertTrue(tmpDelay==oldDelay);
    }    
    
    private static class DelayedReader extends StringReader {

    	private boolean isReady = false;
    	private int maxDelayCount = 3;
    	private int delayCount = 1;
    	
		public DelayedReader(String s) {
			super(s);
		}		
		
		@Override
		public boolean ready() {
			if (delayCount++ > maxDelayCount) isReady = true;
			return isReady;
		}
    	
    }
    
    public void test_delay_with_reader() throws IOException {
    	CSVReaderBuilder crb = new CSVReaderBuilder();
    	StringReader s = 
    		new DelayedReader(
    			crb.join(Arrays.asList(stringHeaders)));
    	CSVReader r = new CSVReader(s);
    	r.setTailDelayMillis(1000); // 1 sec
    	assertTrue(r.hasNext());
    	r.readHeaders();
    	Header[] actualHeaders = r.getHeaders();
    	Header[] expectedHeaders = headers;
    	assertEquals(expectedHeaders, actualHeaders);
    }
    
    public void test_delay_with_reader_with_comments_and_empty_lines() 
        throws IOException 
    {
    	CSVReaderBuilder crb = new CSVReaderBuilder();
    	crb.header("H1").header("H2").header("H3");
    	crb.newRow().column("c1").column("c2").column("c3");
    	crb.newRow().column("# Comment line");
    	crb.newRow().column(""); //Blank line
    	crb.newRow().column("d1").column("d2").column("d3");
    	crb.setSuppressQoutes(true); // Otherwise # isn't first char
    	CSVReader r = new CSVReader(crb.buildReader());
    	assertTrue(r.hasNext());
    	r.readHeaders();
    	Header[] actualHeaders = r.getHeaders();
    	Header[] expectedHeaders = new Header[] {
    			new Header("H1"),
    			new Header("H2"),
    			new Header("H3"),    			
    	};
    	assertEquals(expectedHeaders, actualHeaders);
    	//Check that two rows of data gets returned
    	Map<String, Object> actualRow = r.next();
    	Map<String, Object> expectedRow = 
    		newMap(expectedHeaders, 
    			new Object[] { "c1", "c2", "c3"} );
    	assertSameValues(expectedRow, actualRow);
    	actualRow = r.next();
    	expectedRow = 
    		newMap(expectedHeaders, 
    			new Object[] { "d1", "d2", "d3"} );
    	assertSameValues(expectedRow, actualRow);
    	assertFalse(r.hasNext());
    	try {
    		r.next();
    		fail("Successfully called next() on an empty reader.");
    	} catch (NoSuchElementException e) {
    		//ignore -- expected 
    	}
    }
  
    public void test_get_headers() 
    throws IOException 
	{
		CSVReader r = new CSVReader(new StringReader(""));
        assertNull(r.getHeaders());
	}
    
    public void test_get_headers2() 
    throws IOException 
	{
    	CSVReaderBuilder crb = new CSVReaderBuilder();
		CSVReader r = 
			new CSVReader(
				new StringReader(
					crb.join(Arrays.asList(stringHeaders))));
		r.readHeaders();
        Header[] actual = r.getHeaders();
        assertEquals(headers, actual);
	}
  
    public void test_set_headers_null() 
    throws IOException 
	{
    	CSVReaderBuilder crb = new CSVReaderBuilder();
		CSVReader r = 
			new CSVReader(
				new StringReader(
					crb.join(Arrays.asList(stringHeaders))));
		try {
			r.setHeaders(null);
			fail("Was able to set null headers.");
		} catch (IllegalArgumentException e) {
			//ignore -- expected
		}
	}
    
    public void test_set_headers() 
    throws IOException 
	{
    	CSVReaderBuilder crb = new CSVReaderBuilder();
		CSVReader r = 
			new CSVReader(
				new StringReader(
					crb.join(Arrays.asList(stringHeaders))));
		r.readHeaders();
        Header[] actual = r.getHeaders();
        assertEquals(headers, actual);
        r.setHeaders(defaultHeaders);
        actual = r.getHeaders();        
        assertEquals(defaultHeaders, actual);
        
	}
    
    public void test_set_header_out_of_bounds() 
    throws IOException 
	{
    	CSVReaderBuilder crb = new CSVReaderBuilder();
		CSVReader r = 
			new CSVReader(
				new StringReader(
					crb.join(Arrays.asList(stringHeaders))));
		r.readHeaders();
        Header[] actual = r.getHeaders();
        assertEquals(headers, actual);
        try {
        	r.setHeader(stringHeaders.length, new Header("out-of-bounds"));
        	fail("Able to set an out-of-bounds header element.");
        } catch (IndexOutOfBoundsException e) {
        	//ignore -- expected
        }
	}    
    public void test_set_header_null() 
    throws IOException 
	{
    	CSVReaderBuilder crb = new CSVReaderBuilder();
		CSVReader r = 
			new CSVReader(
				new StringReader(
					crb.join(Arrays.asList(stringHeaders))));
		r.readHeaders();
        Header[] actual = r.getHeaders();
        assertEquals(headers, actual);
        try {
        	r.setHeader(stringHeaders.length-1, null);
        	fail("Able to set a null header element.");
        } catch (IllegalArgumentException e) {
        	//ignore -- expected
        }
	}
    
    public void test_set_header_valid() 
    throws IOException 
	{
    	CSVReaderBuilder crb = new CSVReaderBuilder();
		CSVReader r = 
			new CSVReader(
				new StringReader(
					crb.join(Arrays.asList(stringHeaders))));
		r.readHeaders();
        Header[] actual = r.getHeaders();
        assertEquals(headers, actual);
        Header[] headersClone = headers.clone();
        int last = headersClone.length-1;
        headersClone[last] = new Header("replacement");
       	r.setHeader(last, headersClone[last]);
        actual = r.getHeaders();
        assertEquals(headersClone, actual);       	
	}    

    public void test_remove() 
        throws IOException 
	{
		CSVReader r = 
			new CSVReader(new StringReader("bogus"));
		try { 
			r.remove();
			fail("Successfully called unsupported operation.");
		} catch (UnsupportedOperationException e) {
			// ignore -- expected
		}
	}
    
    protected void assertEquals(Header[] expected, Header[] actual) {
        
        assertEquals(expected.length,actual.length);
        
        for(int i=0; i<expected.length; i++) {
            
            if(!expected[i].equals( actual[i])) {
                
                fail("headers["+i+"], expected ["+expected[i]+"] not ["+actual[i]+"]" );
                
            }
            
        }
        
    }
    
    /**
     * Form data structure modeling an expected (parsed) row.
     * 
     * @param headers
     *            The headers.
     * @param vals
     *            The values (one per header and in the same order).
     *            
     * @return The map containing the appropriate values.
     */
    protected Map<String,Object> newMap(Header[] headers, Object[] vals) {

        assert headers.length==vals.length;
        
        Map<String,Object> map = new TreeMap<String,Object>();
        
        for(int i=0; i<headers.length; i++) {
            
            map.put(headers[i].getName(),vals[i]);
            
        }
        
        assert map.size() == headers.length;
        
        return map;
        
    }
    
    protected void assertSameValues(Map<String,Object> expected, Map<String,Object> actual) {
        
        assertEquals("#of values", expected.size(), actual.size() );
        
        Iterator<String> itr = expected.keySet().iterator();

        while(itr.hasNext()) {
            
            String col = itr.next();
            
            assertTrue("No value: col=" + col, actual.containsKey(col));
            
            Object expectedValue = expected.get(col);

            Object actualValue = actual.get(col);
            
            assertNotNull("Col="+col+" is null.", actualValue);
            
            assertEquals("Col="+col, expectedValue.getClass(), actualValue.getClass());
            
            assertEquals("Col="+col, expectedValue, actualValue);
            
        }
        
    }
    
}
