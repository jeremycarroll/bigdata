package com.bigdata.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CSVReaderBuilder {
	/** <code>List</code> holding the header row, if any. */
    private final List<String> header = new ArrayList<String>();
    /** <code>List</code> of <code>List</code>s holding the columnar data */
    private final List<List<String>>rows = new ArrayList<List<String>>();
    /** <code>List</code> holding the current row being operated on. */
    private final List<String> currentRow = new ArrayList<String>();
    /** Flag determining whether or not to display the header row. Default is false. */
    private boolean suppressHeader = false;
    /** Flag determining whether or not to display quoted content. Default is false. */
    private boolean suppressQoutes = false;
    /** <code>String</code> used for column delimiter. Default is a comma. */
    private String columnDelimiter = ",";
    /** <code>String</code> used for row delimiter. Default is a newline. */
    private String rowDelimter = "\n";
    /** <code>String</code> used for quote delimiter. Default is a double quote. */
    private String quoteDelimter = "\"";

    //Setters and getters
    public String getQuoteDelimter() {
        return quoteDelimter;
    }
    public void setQuoteDelimter(String quoteDelimter) {
        this.quoteDelimter = quoteDelimter;
    }
    public boolean isSuppressQoutes() {
        return suppressQoutes;
    }
    public void setSuppressQoutes(boolean suppressQoutes) {
        this.suppressQoutes = suppressQoutes;
    }
    public String getColumnDelimiter() {
        return columnDelimiter;
    }
    public void setColumnDelimiter(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }
    public String getRowDelimiter() {
        return rowDelimter;
    }
    public void setRowDelimiter(String rowDelimter) {
        this.rowDelimter = rowDelimter;
    }    
    public boolean isSuppressHeader() {
        return suppressHeader;
    }
    public void setSuppressHeader(boolean suppressHeader) {
        this.suppressHeader = suppressHeader;
    }
    
    /** Default constructor */
    CSVReaderBuilder() {}
    
    /** Adds the given string to the header row */
    public CSVReaderBuilder header(String h){header.add(h); return this;}
    
    /** Adds the given string to the current row */
    public CSVReaderBuilder column(String c){currentRow.add(c); return this;}
    
    /** Creates a new row by: 1) flushing the current row data, if any, to the collection
     *  of row data and 2) clearing the the current row.
     */
   public CSVReaderBuilder newRow() {
        flushCurrentRow();
        currentRow.clear();
        return this;
    }
    
    /**
     * Helper method for flushing and clearing the current row.
     */
    private void flushCurrentRow() {
        if (!currentRow.isEmpty()) {
            List<String> t = new ArrayList<String>();
            for (String s: currentRow) {
                t.add(s);
            }        
            rows.add(t);
        }
        currentRow.clear();
    }
    
    /**
     * Creates and returns a <code>Reader</code> object which contains the current set
     * of header (optional) and data rows. The data will be formatted according to the
     * current set of configurable attributes (e.g. delimiter settings).
     * @return Reader which contains the formatted header and data rows.
     */
    public Reader buildReader() {
        StringBuilder sb = new StringBuilder();
        if (!suppressHeader) {
            addHeader(sb);
        }
        flushCurrentRow();
        addRows(sb);
        return new StringReader(sb.toString());
    }
    
    /**
     * Helper method for adding (optional) header data to given <code>StringBuilder</code>.
     * @param sb <code>StringBuilder</code> to append header row.
     */
    private void addHeader(StringBuilder sb) {
        sb.append(join(header));
        sb.append(getRowDelimiter());
    }
    
    /**
     * Helper methos for adding data rows to the given <code>StringBuilder</code>.
     * @param sb <code>StringBuilder</code> to append data rows.
     */
    private void addRows(StringBuilder sb) {
        for (List<String> row: rows) {
            sb.append(join(row));
            sb.append(getRowDelimiter());                
        }
    }
        
    /**
     * Helper method that optionally adds the configured quote delimiter to the given
     * <code>String</code>.
     * @param h The <code>String</code> to optionally quote.
     * @return The optionally quoted <code>String</code>
     */
    private String quote(String h) {
        String quoted = h;
        if (!suppressQoutes) {
            quoted = getQuoteDelimter() + h + getQuoteDelimter(); 
        }
        return quoted;
    }
     
    /**
     * Helper method that joins the given collection of <code>String</code> using the
     * configured column delimiter.
     * @param s the collection of strings to join
     * @return String containing the collection's elements separated by the column delimiter.
     */
    public String join(Collection<String> s) {
        if (s == null || s.isEmpty()) return "";
        Iterator<String> iter = s.iterator();
        StringBuilder builder = new StringBuilder(quote(iter.next()));
        while( iter.hasNext() )
        {
            builder.append(getColumnDelimiter()).append(quote(iter.next()));
        }
        return builder.toString();
    }
    
    /**
     * Test driver method for this class. [Not exhaustive.]
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        CSVReaderBuilder cb = new CSVReaderBuilder();
        cb.header("A").header("B").header("C");
        cb.newRow().column("a").column("b").column("c");
        cb.newRow().column("x").column("y").column("z");
        cb.newRow().column("");
        cb.newRow().column("#");
        Reader r = cb.buildReader();
        System.out.println("Default listing...");        
        listReader(r);
        cb.setSuppressQoutes(true);
        r = cb.buildReader();
        System.out.println("No quotes listing...");        
        listReader(r);
        cb.setSuppressHeader(true);        
        r = cb.buildReader();
        System.out.println("No quotes and no header listing...");        
        listReader(r);
        cb.setColumnDelimiter("\t");        
        r = cb.buildReader();
        System.out.println("No quotes and no header and tab delimited listing...");        
        listReader(r);        
    }
    
    /**
     * Helper method for displaying content of the given <code>Reader</code> 
     * to <code>System.out</code>.
     * @param r the <code>Reader</code> to read from.
     * @throws IOException if there's a problem obtaining data from the <code>Reader</code>.
     */
    private static void listReader(Reader r) throws IOException {
        BufferedReader br = new BufferedReader(r);
        String s = null;
        while((s = br.readLine()) != null) {
            System.out.println(s);
        }    	
    }
}

