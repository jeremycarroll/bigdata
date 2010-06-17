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
package com.bigdata.boot.launcher;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

class ConfigurationPropertyMap implements Cloneable {

    protected TreeMap<String,String> properties = new TreeMap<String,String>();
    protected TreeMap<String,String> javaprops = new TreeMap<String,String>();

    @SuppressWarnings("unchecked")
    public Object clone() throws CloneNotSupportedException {
        ConfigurationPropertyMap copy = 
            (ConfigurationPropertyMap)super.clone();
        copy.properties = (TreeMap)properties.clone();
        copy.javaprops = (TreeMap)javaprops.clone();
        return copy;
    }

    /**
     * Returns an immutable sorted map whose elements are the properties
     * defined in the configuration.
     */
    public SortedMap<String,String> getProperties() {
        return Collections.unmodifiableSortedMap(properties);
    }

    /**
     * Returns an immutable map whose elements are the Java system
     * properties defined in the configuration.
     */
    public Map<String,String> getJavaProps() {
        return Collections.unmodifiableMap(javaprops);
    }

    /**
     * Returns an immutable map whose elements are the environment variables
     * defined by the properties in the configuration. Note that environment
     * variables use the convention of prefixing the environment variable
     * name with string "env.".
     */
    public Map<String,String> getEnvs() {

        Map<String,String> result = new TreeMap<String,String>();

        //while entry is not null and its key starts with "env.",
        for( Map.Entry<String,String> entry = properties.higherEntry("env.");
                        (entry != null) && entry.getKey().startsWith("env.");
            entry = properties.higherEntry(entry.getKey()) )
        {
            result.put(entry.getKey().substring(4), entry.getValue());
        }
        return Collections.unmodifiableMap(result);
    }

    public void setProperty(String name, String value) {
        properties.put(name, value);
    }

    public void setJavaProp(String name, String value) {
        javaprops.put(name, value);
    }

    // throws java.util.NoSuchElementException
    public String substitutePropValues(String s) {

        StringBuilder output = new StringBuilder();
        int i = 0;
        while(i != -1) {
            int next_i = s.indexOf('$', i);
            if(next_i == -1) {
                // No more '$' characters. Output the rest
                output.append(s.substring(i));
                break;
            }

            // Found a '$' - output everything up to it
            output.append(s.substring(i, next_i));

            if(next_i >= s.length()-1) {

                // The input ends with a '$' so just keep it
                output.append('$');
                next_i++;

            } else if(s.charAt(next_i+1) == '$') {

                // Double '$' so only output one
                output.append('$');
                next_i += 2;

            } else if(s.charAt(next_i+1) == '{') {//an actual prop substitution

                int end_i = s.indexOf('}', next_i+2);
                if(end_i == -1) {
                    // It's truncated. So just leave it as-is then
                    output.append(s.substring(next_i));
                    next_i = -1;
                } else {
                    String k = s.substring(next_i+2, end_i);

                    // Check for an already defined property or env variable
                    String v = properties.get(k);
                    if( (v == null) && k.startsWith("env.") ) {
                        // Check the process' environment for a value
                        k = k.substring(4);
                        v = System.getenv(k);
                        if(v == null) {
                            throw new java.util.NoSuchElementException
                                          ("no environment variable for "
                                           +"given name ['"+k+"']");
                        }
                    } else if(v == null) {
                        throw new java.util.NoSuchElementException
                                          ("no property for given name "
                                           +"['"+k+"']");
                    }
                    output.append(v);
                    next_i = end_i + 1;
                }

            } else {
                // This is just a '$'. Nothing special.
                output.append('$');
                next_i++;
            }
            i = next_i;
        }
        return output.toString();
    }
}
