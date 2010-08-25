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

package com.bigdata.util;

import com.sun.jini.lookup.entry.LookupAttributes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import net.jini.lookup.entry.ServiceControlled;
import net.jini.lookup.entry.ServiceInfo;
import net.jini.lookup.entry.ServiceType;
import net.jini.core.entry.Entry;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class containing a set of convenient utility methods that provide
 * general-purpose functions for processing sets of Jini attributes (a Jini
 * (<i>attribute</i> is an instance of <code>net.jini.core.entry.Entry</code>);
 * functions that do things such as displaying information about the
 * contents of one or more entry sets, comparing the contents of two
 * such sets, etc.
 * <p>
 * Note that the set of methods provided in this class is not exhaustive,
 * and is generally focused more on processing and debugging attributes
 * defined specifically for the Acinion system, than attributes defined
 * outside of that system. For general-purpose utility methods that are
 * focused on arbitrarily defined Jini attributes specified for any system,
 * refer to the <code>com.sun.jini.lookup.entry.LookupAttributes</code>
 * class. The methods of this class then, used in conjunction with the
 * methods of <code>LookupAttributes</code>, should provide a more
 * complete set of tools for handling and processing attributes of
 * various class types.
 *
 * @see com.sun.jini.lookup.entry.LookupAttributes
 */
public class EntryUtil {

    /** 
     * Examines the given array of entries and returns the first 
     * element encountered that is an instance of the given class
     * type. If no element in the array is an instance of the given
     * type, then <code>null</code> is returned.
     */
    public static <T> T getEntryByType(Entry[] attrs, Class<T> type)
    {
        if( (attrs == null) || (type == null) ) return null;
        for(int i=0; i<attrs.length; i++) {
            if(type.isInstance(attrs[i]))
                return type.cast(attrs[i]);
        }
        return null;
    }//end getEntryByType

    /** 
     * Examines the given array of entries and returns an array of all
     * elements that are an instance of the given class type. If no element in
     * the array is an instance of the given type, then <code>null</code> is
     * returned.
     */
    public static <T> T[] getEntriesByType(Entry[] attrs, Class<T> type)
    {
        if( (attrs == null) || (type == null) ) return null;
        ArrayList<T> matches = null;
        for(int i=0; i<attrs.length; i++) {
            if( !( type.isInstance(attrs[i]) ) ) continue;
            if ( matches == null )
                matches = new ArrayList<T>();
            matches.add( (T)attrs[i] );
        }//end loop

        if ( matches == null ) return null;

        return matches.toArray
                   ( (T[])java.lang.reflect.Array.newInstance
                                                 ( type, matches.size() ) );
    }

    /** 
     * Examines the given array of entries and returns a list of all
     * elements that are an instance of the given class type. If no element
     * in the array is an instance of the given type, then an empty list is
     * returned.
     */
    public static <T> List<T> getEntryListByType(final Entry[] attrs,
                                                 final Class<T> type)
    {
        if(attrs == null) {
            throw new NullPointerException("null attrs array");
        }
        if(type == null) {
            throw new NullPointerException("null type");
        }
        final List<T> matches = new ArrayList<T>();
        for (final Entry attr : attrs) {
            if(type.isInstance(attr)){
                matches.add(type.cast(attr));
            }
        }
        return matches;
    }

    /** 
     * Examines the given array of entries and returns a list of all
     * elements that match the given <code>template</code>; where 
     * the matching semantics are defined by the template-matching
     * semantics specified in the Lookup Service Specification. If no
     * element in the given array matches the template, then an empty
     * list is returned.
     */
    public static <T extends Entry> List<T> getEntryList(final Entry[] attrs,
                                                         final T template)
    {
        final List<T> matches = new ArrayList<T>();
        for (final Entry attr : attrs) {
            if(LookupAttributes.matches(template, attr)){
                matches.add((T) attr);
            }
        }
        return matches;
    }

    /** 
     * Using the given <code>Logger</code>, displays the contents
     * of the given array of entries.
     */
    public static void displayEntrySet(Entry[] entries, Logger  logger) {
        displayEntrySet(null, entries, null, logger);
    }

    /** 
     * Using the given <code>Logger</code>, displays the contents
     * of the given array of entries.
     * <p>
     * Additionally, the identification of the output can be
     * customized using the given <code>entrySetName</code>.
     */
    public static void displayEntrySet(Entry[] entries,
                                       String  entrySetName,
                                       Logger  logger)
    {
        displayEntrySet(null, entries, entrySetName, logger);
    }

    /** 
     * Using the given <code>Logger</code>, displays the contents
     * of the given array of entries.
     * <p>
     * Each line displayed is prefixed using the value of the
     * <code>prefix</code> parameter.
     */
    public static void displayEntrySet(String  prefix,
                                       Entry[] entries,
                                       Logger  logger)
    {
        displayEntrySet(prefix, entries, null, logger);
    }

    /** 
     * Using the given <code>Logger</code>, displays the contents
     * of the given array of entries.
     * <p>
     * Each line displayed is prefixed using the value of the
     * <code>prefix</code> parameter. Additionally, the identification
     * of the output can be customized using the given
     * <code>entrySetName</code>.
     */
    public static void displayEntrySet(String  prefix,
                                       Entry[] entries,
                                       String  entrySetName,
                                       Logger  logger)
    {
        if ( logger.isDebugEnabled() ) {
            if(prefix == null) prefix = "";
            String name = ((entrySetName == null) ?
                           "Entry Set" : entrySetName);
            if(entries == null) {
                logger.debug(prefix+": "+name+" = null");
            } else if(entries.length <= 0) {
                logger.debug(prefix+": "+name+" =  NO_ENTRIES");
            } else {
                logger.debug(prefix+": "+name);
                logger.debug(prefix+": "
                           +"-- Number of Entries = " + entries.length);
                for(int i=0; i<entries.length; i++) {
                    displayEntry(entries[i], "", logger);
                }//end loop
            }//endif
        }
    }

    /** 
     * Using the given <code>Logger</code>, displays the contents of
     * the given <code>entry</code>: Class, class loader, fields.
     */
    public static void displayEntry(Entry entry, Logger logger) {
        displayEntry(null, entry, null, logger);
    }

    /** 
     * Using the given <code>Logger</code>, displays the contents of
     * the given <code>entry</code>: Class, class loader, fields.
     * <p>
     * Each line displayed is prefixed using the value of the
     * <code>prefix</code> parameter.
     */
    public static void displayEntry(String prefix, 
                                    Entry  entry,
                                    Logger logger)
    {
        displayEntry(prefix, entry, null, logger);
    }

    /** 
     * Using the given <code>Logger</code>, displays the contents of
     * the given <code>entry</code>: Class, class loader, fields.
     * <p>
     * Each field value displayed is prefixed with the value of the
     * <code>label</code> parameter.
     */
    public static void displayEntry(Entry  entry, 
                                    String label, 
                                    Logger logger) 
    {
        displayEntry(null, entry, label, logger);
    }

    /** 
     * Using the given <code>Logger</code>, displays the contents of
     * the given <code>entry</code>: Class, class loader, fields.
     * <p>
     * Each line displayed is prefixed using the value of the
     * <code>prefix</code> parameter, and each field value displayed
     * is prefixed with the value of the <code>label</code> parameter.
     */
    public static void displayEntry(String prefix,
                                    Entry  entry,
                                    String label,
                                    Logger logger)
    {
        if ( logger.isDebugEnabled() )
        {
            if(prefix == null) prefix = "";
            if(label  == null) label  = "";
            if(entry == null) {
                logger.debug(prefix+": Class = null");
                return;
            }//endif
            String classStr = (entry.getClass()).toString();
            int indx = 1 + classStr.lastIndexOf(".");
            try {
                logger.debug(prefix+": "
                             +"Class = " + classStr.substring(indx));
                logger.debug(prefix+": "
                             +"Loader = " + entry.getClass().getClassLoader());
            } catch(IndexOutOfBoundsException e) {
                logger.debug(prefix+": Class = " + classStr);
            }
            Field[] fields = getFieldInfo(entry);
            try {
                for(int i = 0; i < fields.length; i++) {
                    Field  entryField = fields[i];
                    Object entryValue = entryField.get(entry);
                    String entryName  = entryField.getName();
                    logger.debug(prefix+": "
                                 +label
                                 +entryName+" = "+entryValue);
                }//end loop
            } catch (IllegalAccessException e) {
                e.printStackTrace();// should never happen - all fields public
            }
        }
    }

    /** 
     * Using the given <code>Logger</code>, displays fields of the given
     * <code>entry</code> instances that are not equal. Each line displayed
     * is prefixed using the value of the <code>prefix</code> parameter.
     */
    public static void displayDiff(String prefix,
                                   Entry  entry1,
                                   Entry  entry2,
                                   Logger logger)
    {
        if ( logger.isDebugEnabled() ) {
            if(prefix == null) prefix = "";

            String e1Str = (entry1 == null ? 
                                 null : (entry1.getClass()).getName() );
            String e2Str = (entry2 == null ? 
                                 null : (entry2.getClass()).getName() );

            Map<Field, List> diffMap = diff(entry1, entry2);

            if(diffMap == null) {
                logger.log(Level.DEBUG, prefix+": "
                           +"CANNOT BE COMPARED: [entry1="+e1Str+"], "
                           +"[entry2="+e2Str+"]");
                return;
            }//endif

            if(diffMap.size() == 0) {
                logger.log(Level.DEBUG, prefix+": "
                           +"NO DIFFERENCE: [entry1="+e1Str+"], "
                           +"[entry2="+e2Str+"]");
                return;
            }//endif

            Set<Map.Entry<Field, List>> fieldSet = diffMap.entrySet();
            Iterator<Map.Entry<Field, List>> fieldItr = fieldSet.iterator();
            while( fieldItr.hasNext() ) {
                Map.Entry<Field, List> pair = fieldItr.next();
                Field field = pair.getKey();
                List  vals  = pair.getValue();
                String fieldName = field.getName();
                if(vals.size() != 2) {//shouldn't happen
                    logger.log(Level.WARN, "UNEXPECTED ERROR: "
                               +"number of field values != 2 [field="+fieldName
                               +", # of values="+vals.size()+", "
                               +"vals="+vals+"]");
                }//endif
                Object val1 = vals.get(0);
                Object val2 = vals.get(1);
                logger.log(Level.DEBUG, prefix+": "
                           +e1Str+"."+fieldName+" = "+val1+" --> "+val2);
            }//end loop
        }//end(logger.isDebugEnabled)

    }

    public static void displayDiff(Entry entry1, Entry entry2, Logger logger) {
        displayDiff(null, entry1, entry2, logger);
    }

    /** 
     * Compares for equivalence, the contents of two individual entries.
     */
    public static boolean compareEntries(Entry entry1,
					 Entry entry2)
    {
        return ( compareEntrySets(null,
                                  new Entry[] {entry1},
                                  new Entry[] {entry2},
                                  null) );
    }

    /** 
     * Compares for equivalence, the contents of two individual entries.
     * <p>
     * Any lines displayed during the comparision will be displayed
     * using the given <code>Logger</code>.
     */
    public static boolean compareEntries(Entry  entry1,
					 Entry  entry2,
                                         Logger logger)
    {
        return ( compareEntrySets(null,
                                  new Entry[] {entry1},
                                  new Entry[] {entry2},
                                  logger) );
    }

    /** 
     * Compares for equivalence, the contents of two individual entries.
     * <p>
     * Any lines displayed during the comparision will be displayed
     * using the given <code>Logger</code>, and will be prefixed using
     * the value of the <code>prefix</code> parameter.
     */
    public static boolean compareEntries(String prefix,
                                         Entry  entry1,
					 Entry  entry2,
                                         Logger logger)
    {
        return ( compareEntrySets(prefix,
                                  new Entry[] {entry1},
                                  new Entry[] {entry2},
                                  logger) );
    }

    /* ***************************************************************** */
    /* ************************ compareEntrySets *********************** */
    /* ***************************************************************** */
    /**
     * Compares for equivalence, the contents of two sets of entries
     * ignoring duplicate entries.
     */
    public static boolean compareEntrySets(Entry[] entrySet1,
					   Entry[] entrySet2)
    {
        return compareEntrySets(null, entrySet1, entrySet2, null);
    }

    /**
     * Compares for equivalence, the contents of two sets of entries
     * ignoring duplicate entries. 
     * <p>
     * Any lines displayed during the comparision will be displayed
     * using the given <code>Logger</code>.
     */
    public static boolean compareEntrySets(Entry[] entrySet1,
					   Entry[] entrySet2,
                                           Logger  logger)
    {
        return compareEntrySets(null, entrySet1, entrySet2, logger);
    }

    /**
     * Compares for equivalence, the contents of two sets of entries
     * ignoring duplicate entries. 
     * <p>
     * Any lines displayed during the comparision will be displayed
     * using the given <code>Logger</code>, and will be prefixed using
     * the value of the <code>prefix</code> parameter.
     */
    public static boolean compareEntrySets(String  prefix,
                                           Entry[] entrySet1,
					   Entry[] entrySet2,
                                           Logger  logger)
    {
        boolean doDisplay = false;
        if(logger != null) {
            /* If current level is equal to TRACE, then display */
            if( logger.isTraceEnabled() ) doDisplay = true;
            if(prefix == null) prefix = "";
        }//endif

        /* Handle null */
        if(entrySet1 == null) {
            if (entrySet2 == null) {
                return true;
            } else {
                if(doDisplay) {
                    logger.trace(prefix+": entrySet1 = "
                               +"null, entrySet2 != null");
                }//endif
		return false;
            }//endif
        } else {//entrySet1 != null
            if(entrySet2 == null) {
                if(doDisplay) {
                    logger.trace(prefix+": entrySet1 != "
                               +"null, entrySet2 == null");
                }//endif
		return false;
            }//endif
        }//endif

        /* Remove any duplicates in either set of entries */
        HashSet hashSet1 = new HashSet();
        if(entrySet1 != null) {
            for(int i=0; i<entrySet1.length; i++) {
                hashSet1.add(entrySet1[i]);
            }//end loop
        }//endif
        HashSet hashSet2 = new HashSet();
        if(entrySet2 != null) {
            for(int i=0; i<entrySet2.length; i++) {
                hashSet2.add(entrySet2[i]);
            }//end loop
        }//endif
        /* Both sets empty ? */
        if( (hashSet1.size() == 0) && (hashSet2.size() == 0) ) {
            return true;
        }//endif
        /* Determine if the sets have the same number of elements */
        if( hashSet1.size() != hashSet2.size() ) {
            if(doDisplay) {
                logger.trace(prefix
		           +": number of entries not equal ("
		           + hashSet1.size() + " != " + hashSet2.size() + ")");
            }//endif
	    return false;
        }//endif

        /* Determine if the sets have the same content */

        if(doDisplay) {
            logger.trace(prefix+":  ");
            logger.trace(prefix+": comparison loop");
            logger.trace(prefix+":  ");
        }//endif
        int i = 0;
        for(Iterator itr1=hashSet1.iterator(); itr1.hasNext(); i++) {
            Entry entry1 = (Entry)itr1.next();
            if(doDisplay) {
                displayEntry(entry1,"entrySet1["+i+"].",logger);
            }//endif
            int j = 0;
            for(Iterator itr2=hashSet2.iterator(); itr2.hasNext(); j++) {
                Entry entry2 = (Entry)itr2.next();
                if(doDisplay) {
                    displayEntry(entry2, "entrySet2["+j+"].", logger);
                }//endif
                if( equalByContent(entry2,entry1) ||
                    equals(entry1, entry2) )
                {
                    hashSet2.remove(entry2);
                    break;
                }//endif
            }//end loop(entrySet2)

            if(doDisplay) logger.trace(prefix+":  ");

        }//endloop(entrySet1)

        /* The two sets are equal only if, after the above comparison loop,
         * there are no elements left in the second set 
         */
        if(hashSet2.size() != 0) {
            if(doDisplay) {
                logger.trace(prefix
                           +": entries left over from comparison loop, "
		           + "entry sets not equal");
            }//endif
	    return false;
        }//endif

	return true;
    }

    /** Returns public, non-static, non-transient, non-final fields contained
     *  in the given <code>entry</code> parameter
     */
    public static Field[] getFieldInfo(Entry entry) {
        final int SKIP_MODIFIERS
                   = (Modifier.STATIC | Modifier.TRANSIENT | Modifier.FINAL);
        /* Scan the array to see if it can be used as-is or if a smaller
         * array must be constructed because some fields must be skipped.
         * If so, then create an ArrayList and add unskippable fields to it;
         * then fetch the array back out of it.
         */
        Field[] fields = entry.getClass().getFields();
        ArrayList usable = null;
        for (int i = 0; i < fields.length; i++) {
            // exclude this one?
            if ((fields[i].getModifiers() & SKIP_MODIFIERS) != 0) {
                if (usable == null) {           //first excluded: set up for it
                    usable = new ArrayList();   //allocate the list of usables
                    for (int j = 0; j < i; j++){//earlier fields are usable
                        usable.add(fields[j]);
                    }
                }
            } else {                            // not excluded
                if (usable != null) {           // tracking usable fields?
                    usable.add(fields[i]);
                }
            }
        }
        if (usable != null) {
            fields = (Field[]) usable.toArray(new Field[usable.size()]);
        }
        return fields;
    }

    /**
     * For entries which may be preferred, but which also may be
     * visible on the application's classpath (which may cause a
     * standard equality check to fail because a1 and a2 are loaded
     * by different class loaders), this method tests the entries
     * for content equality.
     */
    private static boolean equalByContent(Entry a1, Entry a2) {
	if (a1 instanceof ServiceType && a2 instanceof ServiceType) {
	    ServiceType t1 = (ServiceType)a1;
	    ServiceType t2 = (ServiceType)a2;
	    if( t1.getDisplayName() != null ) {
		if( !t1.getDisplayName().equals(t2.getDisplayName()) ) {
		    return false;
		}//endif 
	    } else if( t2.getDisplayName() != null ) {
		    return false;
	    }//endif
	    if( t1.getShortDescription() != null ) {
		if( !t1.getShortDescription().equals
                                             (t2.getShortDescription()) )
                {
		    return false;
		}//endif
	    } else if( t2.getShortDescription() != null ) {
                return false;
	    }//endif
	    return true;
	}//endif
	return false;
    }

    /**
     * Convenience method that determines if the given <code>Entry</code>
     * instances are equal, by comparing the public fields of each parameter.
     */
    private static boolean equals(Entry e1, Entry e2) {
        if (e1 == e2) return true;
        if (e1 == null || e2 == null) return false;
        if (e1.getClass() != e2.getClass()) return false;

        // compare each field
        Field[] fields = getFieldInfo(e1);//returns only the public fields
        try {
            for(int i=0; i<fields.length; i++) {
                Field f = fields[i];
                //can use same Field f for both e1 & e2 since same Entry type
                Object f1 = f.get(e1);
                Object f2 = f.get(e2);

                if(f2 == f1) continue;//same obj or both null is OK
                if(f2 == null || f1 == null) return false;//only one null !OK

                if( !f2.equals(f1) ) {//test for arrays
                    Class f2Class = f2.getClass();
                    Class f1Class = f1.getClass();
                    if( f2Class.isArray() && f1Class.isArray() ) {
                        //determine if they are Object or primitive arrays
                        Class f2Type = f2Class.getComponentType();
                        Class f1Type = f1Class.getComponentType();

                        if( !f2Type.equals(f1Type) ) return false;//diff types

                        boolean equalArrays = false;
                        if( !f2Type.isPrimitive() && !f1Type.isPrimitive() ) {

                            //types are equal & both non-primitive, use Object
                            equalArrays = 
                                  Arrays.equals( (Object[])f2, (Object[])f1 );

                        } else if(f2Type.isPrimitive()&&f1Type.isPrimitive()){

                            //types are equal & both primitive, use primitive
                            if( f2Type.equals(Boolean.TYPE) ) {
                                equalArrays = Arrays.equals( (boolean[])f2,
                                                             (boolean[])f1 );
                            } else if( f2Type.equals(Character.TYPE) ) {
                                equalArrays = Arrays.equals( (char[])f2,
                                                             (char[])f1 );
                            } else if( f2Type.equals(Byte.TYPE) ) {
                                equalArrays = Arrays.equals( (byte[])f2,
                                                             (byte[])f1 );
                            } else if( f2Type.equals(Short.TYPE) ) {
                                equalArrays = Arrays.equals( (short[])f2,
                                                             (short[])f1 );
                            } else if( f2Type.equals(Integer.TYPE) ) {
                                equalArrays = Arrays.equals( (int[])f2,
                                                             (int[])f1 );
                            } else if( f2Type.equals(Long.TYPE) ) {
                                equalArrays = Arrays.equals( (long[])f2,
                                                             (long[])f1 );
                            } else if( f2Type.equals(Float.TYPE) ) {
                                equalArrays = Arrays.equals( (float[])f2,
                                                             (float[])f1 );
                            } else if( f2Type.equals(Double.TYPE) ) {
                                equalArrays = Arrays.equals( (double[])f2,
                                                             (double[])f1 );
                            }//endif

                        //else 1 primitive, 1 not primitive ==> !equalArrays

                        }//endif

                        if( !equalArrays ) return false;//cur fields not equal
                        continue;//cur fields equal, go to next field

                    }//endif( f2Class.isArray() && f1Class.isArray() )

                    return false;//(not arrays && !f2.equals(f1))

                }//endif( !f2.equals(f1) )

            }//end loop

            return true;//all corresponding fields equal

        } catch (IllegalAccessException e) {
            // should never happen, all entry fields are public
            throw new AssertionError(e);
        }
    }

    /**
     * Convenience method that, given two <code>Entry</code> instances that
     * are of the <i>same type</i>, determines which of the corresponding
     * public fields of the given <code>Entry</code> instances are not equal,
     * and returns a <code>Map</code> in which each key-value pair contains
     * a key represented by the <code>Field</code> being compared, and a
     * corresponding value represented by a <code>List</code> whose
     * elements are the two values of the fields being compared; where the
     * first element of the <code>List</code> is the value of the field
     * in the first given <code>Entry</code>, and the second element of
     * the <code>List</code> is the value of the field in the second
     * <code>Entry</code> parameter.
     * <p>
     * If the <code>Entry</code> values input to this method are not the
     * same type, then <code>null</code> is returned. If <code>null</code>
     * is input for either or both of the <code>Entry</code> instances,
     * then <code>null</code> is also returned. Finally, if the value of
     * each public field of the first parameter equals the value of the
     * corresponding field of the second parameter, then an empty
     * <code>Map</code> is returned.
     */
    public static Map<Field, List> diff(Entry e1, Entry e2) {
        if (e1 == null || e2 == null) return null;
        if (e1.getClass() != e2.getClass()) return null;//must be same type

        Map<Field, List> retMap = new LinkedHashMap<Field, List>();

        if (e1 == e2) return retMap;

        // compare each field
        Field[] fieldsArray1 = getFieldInfo(e1);//returns public fields
        Field[] fieldsArray2 = getFieldInfo(e2);//returns public fields
        try {
            for(int i=0; i<fieldsArray1.length; i++) {

                Field field1 = fieldsArray1[i];
                Field field2 = fieldsArray2[i];

                Object f1 = field1.get(e1);//value of field1
                Object f2 = field2.get(e2);//value of field2

                List fieldVals = new ArrayList();

                if(f2 == f1) continue;//same obj or both null, f1 == f2, skip

                if(f2 == null || f1 == null) {//only 1 is null, f1 != f2
                    fieldVals.add(f1);
                    fieldVals.add(f2);
                    retMap.put(field1, fieldVals);
                    continue;//next field
                }//endif

                if( !f2.equals(f1) ) {//test for arrays
                    Class f2Class = f2.getClass();
                    Class f1Class = f1.getClass();
                    if( f2Class.isArray() && f1Class.isArray() ) {
                        //determine if they are Object or primitive arrays
                        Class f2Type = f2Class.getComponentType();
                        Class f1Type = f1Class.getComponentType();

                        if( !f2Type.equals(f1Type) ) {//field types not equal
                            fieldVals.add(f1);
                            fieldVals.add(f2);
                            retMap.put(field1, fieldVals);
                            continue;//next field
                        }//endif

                        boolean equalArrays = false;
                        if( !f2Type.isPrimitive() && !f1Type.isPrimitive() ) {

                            //types are equal & both non-primitive, use Object
                            equalArrays = 
                                  Arrays.equals( (Object[])f2, (Object[])f1 );

                        } else if(f2Type.isPrimitive()&&f1Type.isPrimitive()){

                            //types are equal & both primitive, use primitive
                            if( f2Type.equals(Boolean.TYPE) ) {
                                equalArrays = Arrays.equals( (boolean[])f2,
                                                             (boolean[])f1 );
                            } else if( f2Type.equals(Character.TYPE) ) {
                                equalArrays = Arrays.equals( (char[])f2,
                                                             (char[])f1 );
                            } else if( f2Type.equals(Byte.TYPE) ) {
                                equalArrays = Arrays.equals( (byte[])f2,
                                                             (byte[])f1 );
                            } else if( f2Type.equals(Short.TYPE) ) {
                                equalArrays = Arrays.equals( (short[])f2,
                                                             (short[])f1 );
                            } else if( f2Type.equals(Integer.TYPE) ) {
                                equalArrays = Arrays.equals( (int[])f2,
                                                             (int[])f1 );
                            } else if( f2Type.equals(Long.TYPE) ) {
                                equalArrays = Arrays.equals( (long[])f2,
                                                             (long[])f1 );
                            } else if( f2Type.equals(Float.TYPE) ) {
                                equalArrays = Arrays.equals( (float[])f2,
                                                             (float[])f1 );
                            } else if( f2Type.equals(Double.TYPE) ) {
                                equalArrays = Arrays.equals( (double[])f2,
                                                             (double[])f1 );
                            }//endif

                        //else 1 primitive, 1 not primitive ==> !equalArrays

                        }//endif

                        if( !equalArrays ) {//f1 not equal f2
                            fieldVals.add(f1);
                            fieldVals.add(f2);
                            retMap.put(field1, fieldVals);
                            continue;//next field
                        }//endif

                        continue;//f1 equals f2, skip it and go to next field

                    }//endif( f2Class.isArray() && f1Class.isArray() )

                    //(not arrays && !f2.equals(f1))
                    fieldVals.add(f1);
                    fieldVals.add(f2);
                    retMap.put(field1, fieldVals);

                }//endif( !f2.equals(f1) )

            }//end loop

            return retMap;

        } catch (IllegalAccessException e) {
            // should never happen, all entry fields are public
            throw new AssertionError(e);
        }
    }

    /**
     * Convenience method that, given two <code>Entry</code> instances,
     * copies the values of the <i>usable</i> (<code>public</code>,
     * non-<code>transient</code>, non-<code>static</code>,
     * non-<code>final</code>) fields of the first <code>Entry</code>
     * parameter to the usable fields of the same name to the second given
     * <code>Entry</code> parameter. Any usable field from one of the
     * parameters that does not have a counterpart in the other parameter
     * will be by passed. This method can be useful, for example, when
     * converting an <code>AcinionAttribute</code> to a parallel
     * <code>AcinionEvent</code> for reporting outside of the system.
     * If <code>null</code> is input for either parameter, then 
     * <code>null</code> is returned. 
     */
    public static Entry copyEntry(Entry e1, Entry e2, Logger logger) {
        if (e1 == null || e2 == null) return null;
        Entry retEntry = e2;
        String e1Name = (e1.getClass()).getSimpleName();
        String retEntryName = (retEntry.getClass()).getSimpleName();

        Field[] e1Fields = EntryUtil.getFieldInfo(e1);
        Field[] retEntryFields = EntryUtil.getFieldInfo(retEntry);
        if( e1Fields == null || e1Fields.length == 0 ) return null;
        if( retEntryFields == null || retEntryFields.length == 0 ) return null;
        for(int i=0; i<e1Fields.length; i++) {
            Field e1Field = e1Fields[i];
            String e1FieldName = e1Field.getName();
            for(int j=0; j<retEntryFields.length; j++) {
                Field retEntryField = retEntryFields[j];
                String retEntryFieldName = retEntryField.getName();
                String errStr = (logger == null ? null :
                                           "cannot set "
                                           +retEntryName+"."+retEntryFieldName
                                           +" to value of "
                                           +e1Name+"."+e1FieldName );
                if( e1FieldName.equals(retEntryFieldName) ) {
                    try {
                        retEntryField.set( retEntry, e1Field.get(e1) );
                    } catch(IllegalArgumentException e) {
                        if(logger != null) logger.log(Level.WARN, errStr);
                        e.printStackTrace();
                    } catch(IllegalAccessException e) {
                        if(logger != null) logger.log(Level.WARN, errStr);
                        e.printStackTrace();
                    }
                }//endif
            }//end loop(j)
        }//end loop(i)
        return retEntry;
    }

    /**
     * Returns an instance of the given <code>Entry</code> class with
     * all <i>usable</i> (public, non-static, non-transient, non-final)
     * fields set to <code>null</code>. The object returned by this method
     * can be used as a template that will match any <code>Entry</code>
     * that is of the same type as the <code>Entry</code> input to this
     * method.
     *
     * This method can be useful when working with an <code>Entry</code>
     * instance that defines a no-arg constructor that sets one or more
     * of its fields to a non-<code>null</code> value.
     */
    public static Entry wildcardAll(Entry entry) {
        try {
            Class realClass = entry.getClass();
            Entry template = (Entry) realClass.newInstance();

            Field[] f = realClass.getFields();
            for(int i = 0; i < f.length; i++) {
                if(! usableField(f[i])) continue;
                f[i].set(template, null);
            }//end loop
            return template;
        } catch (Throwable t) {
            Logger.getLogger(EntryUtil.class).error("wildcardEntry failed", t);
        }
        return null;
    }

    /**
     * Returns a clone of the given {@link Entry}.
     * (From com.sun.jini.example.browser.ServiceEditor.)
     */
    public static Entry cloneEntry(Entry attr) {
        try {
            Class realClass = attr.getClass();
            Entry template = (Entry) realClass.newInstance();

            Field[] f = realClass.getFields();
            for(int i = 0; i < f.length; i++) {
                if(! usableField(f[i]))
                    continue;
                f[i].set(template, f[i].get(attr));
            }

            return template;
        } catch (Throwable t) {
            Logger.getLogger(EntryUtil.class).error("duplicating entry failed", t);
        }
        return null;
    }

    // from EntryRep
    private static boolean usableField(Field field) {
        Class desc = field.getDeclaringClass();

        if(desc.isPrimitive()) {
            throw new IllegalArgumentException(
                "Primitive types not allowed in an Entry: " + field.getName() );
        }

        // skip anything that isn't a public per-object mutable field
        int mods = field.getModifiers();
        return (0 == (mods & (Modifier.TRANSIENT | Modifier.STATIC | Modifier.FINAL)));
    }
}
