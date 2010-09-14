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
package com.bigdata.test.util;


/**
 * Utility class that extends JUnit's Assert class with additional utilities.
 * 
 * @author blevine
 *
 */
public class Assert extends org.junit.Assert
{
    public static void assertEquals(byte[] expected, byte[] actual)
    {
        assertEquals(null, expected, actual);
    }
    public static void assertEquals(String message, byte[] expected, byte[] actual)
    {
        if (expected == null && actual == null)
        {
            return;
        }
        
        if ( (expected == null) && (actual != null) )
        {
            assertNull(message, actual);
        }
        
        if ( (expected != null) && (actual == null) )
        {
            assertNotNull(message, actual);
        }
        
        if (expected.length != actual.length)
        {
            String msg = "(array lengths do not match).";
            
            if (message != null)
            {
                msg = message + " " + msg;
            }
            
            fail(msg);
        }
        
        for (int i = 0; i < expected.length; i++)
        {
            if (expected[i] != actual[i])
            {
                String msg = "(index = i).";
                
                if (message != null)
                {
                    msg = message + " " + msg;
                }
                assertEquals(msg, expected[i], actual[i]);
            }
        }
    }
}
