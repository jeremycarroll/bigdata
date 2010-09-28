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
 * Created on Jun 20, 2008
 */

package com.bigdata.relation.rule;

import com.bigdata.io.SerializerUtil;
import com.bigdata.relation.rule.Var;
import com.bigdata.test.Assert;
import org.junit.Test;

/**
 * Test suite for {@link Var}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestVar extends Assert {

    /**
     * 
     */
    public TestVar() {
    }

    /**
     * Test the singleton factory for {@link Var}s.
     */
    @Test
    public void test_variableSingletonFactory() {
        
        final Var u = Var.var("u");

        // same instance.
        assertTrue(u == Var.var("u"));
        
        // different instance.
        assertTrue(u != Var.var("x"));
        
        assertTrue(u.equals(Var.var("u")));

        assertFalse(u.equals(Var.var("x")));
        
    }

    @Test
    public void test_variableSingletonDeserialization() {
        
        final Var expected = Var.var("u");

        final byte[] b = SerializerUtil.serialize(expected);
        
        final Var actual = (Var) SerializerUtil.deserialize(b);
        
        assertTrue(expected == actual);
        
    }
    
}
