package com.bigdata.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import net.jini.core.entry.Entry;
import net.jini.entry.AbstractEntry;
import net.jini.lookup.entry.Address;
import net.jini.lookup.entry.Comment;
import net.jini.lookup.entry.Location;
import net.jini.lookup.entry.Name;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestEntryUtil {
    @Test
	public void testGetEntryByType_with_null_args() 
	    throws SecurityException, NoSuchMethodException, 
	           IllegalArgumentException, IllegalAccessException, InvocationTargetException 
	{
		Object[][] cmdLines = {
				{null, null},
				{null, Name.class},
				{new Entry[] {}, null}
		};
		for (Object[] args: cmdLines) {
			@SuppressWarnings("unchecked")
			Object r = EntryUtil.getEntryByType(
				(Entry[])args[0], (Class<? extends Entry>)args[1]);
			assertNull(r);
		}
	}
    @Test
	public void testGetEntryByType_with_emtpy_args() 
    throws SecurityException, NoSuchMethodException, 
           IllegalArgumentException, IllegalAccessException, InvocationTargetException 
	{
		Name t = EntryUtil.getEntryByType(new Entry[]{}, Name.class);
		assertNull(t);
	}
    @Test
	public void testGetEntryByType_no_match() 
    throws SecurityException, NoSuchMethodException, 
           IllegalArgumentException, IllegalAccessException, InvocationTargetException 
	{
		Entry[] entries = new Entry[] {
				new Address(),
				new Comment(),
				new Location()
		};
		Name t = EntryUtil.getEntryByType(entries, Name.class);
		assertNull(t);
	}
    @Test
	public void testGetEntryByType_match() 
    throws SecurityException, NoSuchMethodException, 
           IllegalArgumentException, IllegalAccessException, InvocationTargetException 
	{
		Entry[] entries = new Entry[] {
				new Address(),
				new Comment(),
				new Location()
		};
		Location t = EntryUtil.getEntryByType(entries, Location.class);
		assertNotNull(t);
	}
    @Test
	public void testDisplayEntryEntryLogger() {
	    EntryUtil.displayEntry(
	    	new Address(),
	    	getLevelLogger(Level.DEBUG));
	}
    @Test
	public void testDisplayEntryEntryStringLogger() {
	    EntryUtil.displayEntry(
	    		new Location(),
	    		"Label",
		    	getLevelLogger(Level.DEBUG));
	}
    @Test
	public void testDisplayEntryStringEntryStringLogger() {
	    EntryUtil.displayEntry(
	    		"Prefix",
	    		new Comment("This is a comment."),
                "Label",
    	    	getLevelLogger(Level.DEBUG));
	}
    @Test
	public void testDisplayEntryStringEntryStringLogger_null() {
	    EntryUtil.displayEntry(
	    		null,
	    		null,
                null,
    	    	getLevelLogger(Level.DEBUG));
	}	
	
	private static void assertNotEquivalentEntries(Entry entry1, Entry entry2) {
		assertFalse(
			EntryUtil.compareEntries(
				entry1,
				entry2,
				getLevelLogger(Level.TRACE)));
	}
    @Test
	public void testCompareEntries_not_equal_null() {
		Entry entry1 = null;
		Entry entry2 = new Name();
		assertNotEquivalentEntries(entry1, entry2);
		assertNotEquivalentEntries(entry2, entry1);
	}
    @Test
	public void testCompareEntries_not_equal_diff_type() {
		Entry entry1 = new Name();
		Entry entry2 = new Address();
		assertNotEquivalentEntries(entry1, entry2);
		assertNotEquivalentEntries(entry2, entry1);
	}
    @Test
	public void testCompareEntries_not_equal_diff_content() {
		Entry entry1 = new Name("Name1");
		Entry entry2 = new Name("Name2");
		assertNotEquivalentEntries(entry1, entry2);
		assertNotEquivalentEntries(entry2, entry1);
	}
	
	private static void assertEquivalentEntries(Entry entry1, Entry entry2) {
		assertTrue(
			EntryUtil.compareEntries(
				entry1,
				entry2,
				getLevelLogger(Level.TRACE)));
	}
    @Test
	public void testCompareEntries_equal_null() {
		Entry entry1 = null;
		Entry entry2 = null;
		assertEquivalentEntries(entry1, entry2);
		assertEquivalentEntries(entry2, entry1);
	}
    @Test
	public void testCompareEntries_equal_same_content() {
		Entry entry1 = new Name("Name1");
		Entry entry2 = new Name("Name1");
		assertEquivalentEntries(entry1, entry2);
		assertEquivalentEntries(entry2, entry1);
	}
    @Test
	public void testCompareEntrySets_equiv_null() {
		Entry[] entries1 = null;
		Entry[] entries2 = null;
	    assertEquivalentSets(entries1, entries2);
	    assertEquivalentSets(entries2, entries1);
	}
    @Test
	public void testCompareEntrySets_equiv_empty() {
		Entry[] entries1 = new Entry[] {};
		Entry[] entries2 = new Entry[] {};
	    assertEquivalentSets(entries1, entries2);
	    assertEquivalentSets(entries2, entries1);
	}
    @Test
	public void testCompareEntrySets_equiv_non_empty_singleton() {
		Entry[] entries1 = new Entry[] {new Address()};
		Entry[] entries2 = new Entry[] {new Address()};
	    assertEquivalentSets(entries1, entries2);
	    assertEquivalentSets(entries2, entries1);
	}
    @Test
	public void testCompareEntrySets_equiv_non_empty_mulitple() {
		Entry[] entries1 = new Entry[] {new Address(), new Name(), new Location()};
		Entry[] entries2 = new Entry[] {new Address(), new Name(), new Location()};
	    assertEquivalentSets(entries1, entries2);
	    assertEquivalentSets(entries2, entries1);
	}
    @Test
	public void testCompareEntrySets_equiv_non_empty_mulitple_and_dups() {
		Entry[] entries1 = 
			new Entry[] {new Address(), new Name(), new Location(),
				new Location(), new Name()};
		Entry[] entries2 = 
			new Entry[] {new Address(), new Name(), new Location(),
				new Address()};
	    assertEquivalentSets(entries1, entries2);
	    assertEquivalentSets(entries2, entries1);
	}
		
	private static void assertEquivalentSets(Entry[] entries1, Entry[] entries2) {
		assertTrue(
			EntryUtil.compareEntrySets("Equivalent",
				entries1,
				entries2,
				getLevelLogger(Level.TRACE)));
	}
	
	private static void assertNotEquivalentSets(Entry[] entries1, Entry[] entries2) {
		assertFalse(
			EntryUtil.compareEntrySets("Not equivalent",
				entries1,
				entries2,
				getLevelLogger(Level.TRACE)));
	}
    @Test
	public void testCompareEntrySets_unequiv_null() {
		Entry[] entries1 = null;
		Entry[] entries2 = new Entry[] {};
	    assertNotEquivalentSets(entries1, entries2);
	    assertNotEquivalentSets(entries2, entries1);
	}
    @Test
	public void testCompareEntrySets_unequiv_non_empty_singleton() {
		Entry[] entries1 = new Entry[] {new Comment("C1")};
		Entry[] entries2 = new Entry[] {new Comment("C2")};
	    assertNotEquivalentSets(entries1, entries2);
	    assertNotEquivalentSets(entries2, entries1);
	}
    @Test
	public void testCompareEntrySets_unequiv_non_empty_diff_size() {
		Entry[] entries1 = new Entry[] {new Comment("C1")};
		Entry[] entries2 = new Entry[] {new Comment("C2"), new Comment("C3")};
	    assertNotEquivalentSets(entries1, entries2);
	    assertNotEquivalentSets(entries2, entries1);
	}
	
	private static class MyEntryWithUnusableFields extends AbstractEntry {
		private static final long serialVersionUID = 1L; // final, static, excluded
		public final String finalString = "finalString"; // final, excluded
		public transient String transientString = "transientString"; // trans, excluded
		public static String staticString = "staticString"; // static, excluded
		private String privateString = "privateString"; // private, excluded
		public String  publicString = "publicString"; // included
	}
    @Test
	public void testGetFieldInfo() {
		MyEntryWithUnusableFields mf = new MyEntryWithUnusableFields();
		Field[] fields = EntryUtil.getFieldInfo(mf);	
		assertTrue(fields.length==1);
		assertTrue(fields[0].getName().equals("publicString"));
	}
	
	private static Logger getLevelLogger(Level level) {
		Logger logger = getLogger();
		logger.setLevel(level);
		return logger;
	}
	private static Logger getLogger() {
		return Logger.getLogger(TestEntryUtil.class);
	}
	
}
