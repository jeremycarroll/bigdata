/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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
 * Created on Oct 24, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

/**
 * Test suite for UNION and MINUS combined, see 
 * https://sourceforge.net/apps/trac/bigdata/ticket/767
 * 
 */
public class TestUnionMinus extends AbstractInlineSELECTTestCase {

    /**
     * 
     */
    public TestUnionMinus() {
    }

    /**
     * @param name
     */
    public TestUnionMinus(String name) {
        super(name);
    }

    @Override
    public String trigData() {
    	return  "{                  \r\n" +
    			":a :b \"ab\" .     \r\n" +
    			":a :c \"ac\" .     \r\n" +
    			":a :d \"ad\" .     \r\n" +
                "}";
    }
    

    public void test_union_minus_01() throws Exception {
    	// Concerning omitting the test with hash joins, see Trac776 and 
    	// com.bigdata.rdf.internal.encoder.AbstractBindingSetEncoderTestCase.test_solutionWithOneMockIV()
  
        new Execute(
        		"SELECT  ?s                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   {                      \r\n" + 
        		"     BIND ( :bob as ?s )  \r\n" + 
        		"   } UNION {              \r\n" + 
        		"   }                      \r\n" + 
        		"   MINUS {                \r\n" + 
        		"      BIND ( :bob as ?s ) \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?s","UNDEF");
        
    }

   
    public void test_union_minus_02() throws Exception {

    	new Execute(
        		"SELECT  ?s\r\n" + 
        		"WHERE {\r\n" + 
        		"   { \r\n" + 
        		"     BIND ( :bob as ?s )\r\n" + 
        		"   } UNION {\r\n" + 
        		"   }\r\n" + 
        		"   FILTER (!BOUND(?s) || ?s != :bob)\r\n" + 
        		"}").expectResultSet("?s","UNDEF");
        
    }
    public void test_union_minus_03() throws Exception {

        new Execute(
        		"SELECT  ?s                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   {                      \r\n" + 
        		"     BIND ( 2 as ?s )     \r\n" + 
        		"   } UNION {              \r\n" + 
        		"   }                      \r\n" + 
        		"   MINUS {                \r\n" + 
        		"      BIND ( 2 as ?s )    \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?s","UNDEF");
        
    }
    public void test_union_minus_04() throws Exception {

        new Execute(
        		"SELECT  ?x                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   BIND ( 3 as ?x )       \r\n" + 
        		"   { BIND ( 4 as ?x )     \r\n" + 
        		"   } UNION {              \r\n" + 
        		"     MINUS {              \r\n" + 
        		"      BIND ( 3 as ?x )    \r\n" + 
        		"     }                    \r\n" + 
        		"     BIND (3 as ?x)       \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?x","3");
        
    }
    public void test_union_minus_05() throws Exception {

        new Execute(
        		"SELECT  ?x                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   BIND ( 3 as ?x )       \r\n" + 
        		"   { BIND ( 4 as ?x )     \r\n" + 
        		"   } UNION {              \r\n" + 
        		"     MINUS {              \r\n" + 
        		"      BIND ( 3 as ?x )    \r\n" + 
        		"     }                    \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?x","3");
        
    }
    public void test_union_minus_06_bind() throws Exception {

        new Execute(
        		"SELECT  ?x                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   BIND ( 3 as ?x )       \r\n" + 
        		"   { BIND ( 4 as ?x )     \r\n" + 
        		"   } UNION {              \r\n" + 
        		"     BIND (3 as ?x)       \r\n" + 
        		"     MINUS {              \r\n" + 
        		"      BIND ( 3 as ?x )    \r\n" + 
        		"     }                    \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?x");
        
    }

    public void test_union_minus_06_spo() throws Exception {

        new Execute(
        		"SELECT  ?x                \r\n" + 
        		"WHERE {                   \r\n" + 
        		"   :a :b ?x   .           \r\n" + 
        		"   { :a :c ?x   .         \r\n" + 
        		"   } UNION {              \r\n" + 
        		"     :a :b ?x   .         \r\n" + 
        		"     MINUS {              \r\n" + 
        		"       :a :b ?x   .       \r\n" + 
        		"     }                    \r\n" + 
        		"   }                      \r\n" + 
        		"}").expectResultSet("?x");
        
    }

    public void test_union_minus_07_spo() throws Exception {

        new Execute(
        		"SELECT  ?x                   \r\n" + 
        		"WHERE {                      \r\n" + 
        		"   :a  :b  ?x     .          \r\n" + 
        		"   { :a  :c  ?x     .        \r\n" + 
        		"   } UNION {                 \r\n" + 
        		"     :a  :b  ?x     .        \r\n" + 
        		"     MINUS {                 \r\n" + 
        		"       {                     \r\n" + 
        		"         :a  :b  ?x     .    \r\n" + 
        		"       } UNION {             \r\n" + 
        		"         :a  :c  ?y     .    \r\n" + 
        		"       }                     \r\n" + 
        		"     }                       \r\n" + 
        		"   }                         \r\n" + 
        		"}").expectResultSet("?x");
        
    }
    public void test_union_minus_07_bind() throws Exception {

        new Execute(
        		"SELECT  ?x                   \r\n" + 
        		"WHERE {                      \r\n" + 
        		"   BIND ( 3 as ?x )          \r\n" + 
        		"   { BIND ( 4 as ?x )        \r\n" + 
        		"   } UNION {                 \r\n" + 
        		"     BIND ( 3 as ?x )        \r\n" + 
        		"     MINUS {                 \r\n" + 
        		"       {                     \r\n" + 
        		"         BIND ( 3 as ?x )    \r\n" + 
        		"       } UNION {             \r\n" + 
        		"         BIND ( 4 as ?y )    \r\n" + 
        		"       }                     \r\n" + 
        		"     }                       \r\n" + 
        		"   }                         \r\n" + 
        		"}").expectResultSet("?x");
        
    }
    public void test_union_minus_08_bind() throws Exception {

        new Execute(
        		"SELECT  ?x                   \r\n" + 
        		"WHERE {                      \r\n" + 
        		"   BIND ( 3 as ?x )          \r\n" + 
        		"   { BIND ( 4 as ?x )        \r\n" + 
        		"   } UNION {                 \r\n" + 
        		"     BIND ( 3 as ?x )        \r\n" + 
        		"     MINUS {                 \r\n" + 
        		"       {                     \r\n" + 
        		"         BIND ( 3 as ?x )    \r\n" + 
        		"       } UNION {             \r\n" + 
        		"       }                     \r\n" + 
        		"     }                       \r\n" + 
        		"   }                         \r\n" + 
        		"}").expectResultSet("?x");
        
    }
    public void test_union_minus_08_spo() throws Exception {

        new Execute(
        		"SELECT  ?x                   \r\n" + 
        		"WHERE {                      \r\n" + 
        		"   :a  :b  ?x     .          \r\n" + 
        		"   { :a  :c  ?x     .        \r\n" + 
        		"   } UNION {                 \r\n" + 
        		"     :a  :b  ?x     .        \r\n" + 
        		"     MINUS {                 \r\n" + 
        		"       {                     \r\n" + 
        		"         :a  :b  ?x     .    \r\n" + 
        		"       } UNION {             \r\n" + 
        		"       }                     \r\n" + 
        		"     }                       \r\n" + 
        		"   }                         \r\n" + 
        		"}").expectResultSet("?x");
        
    }
    

    public void test_union_minus_09() throws Exception {

        new Execute(
        		"SELECT  ?x                   \r\n" + 
        		"WHERE {                      \r\n" + 
        		"   BIND ( 3 as ?x )          \r\n" + 
        		"   { BIND ( 4 as ?x )        \r\n" + 
        		"   } UNION {                 \r\n" + 
        		"     BIND ( 3 as ?x )        \r\n" + 
        		"     MINUS {                 \r\n" + 
        		"     }                       \r\n" + 
        		"   }                         \r\n" + 
        		"}").expectResultSet("?x","3");
        
    }

    public void test_union_minus_10() throws Exception {

        new Execute(
        		"SELECT *                     \r\n" + 
        		"WHERE {                      \r\n" + 
        		"  { BIND ( 3 as ?x ) }       \r\n" + 
        		"  UNION                      \r\n" + 
        		"  { BIND ( 4 as ?y ) }       \r\n" + 
        		"  MINUS {                    \r\n" + 
        		"    { BIND ( 3 as ?x ) }     \r\n" + 
        		"    UNION                    \r\n" + 
        		"    { BIND ( 4 as ?y ) }     \r\n" + 
        		"  }                          \r\n" + 
        		"}").expectResultSet("?x ?y");
        
    }
   /* 
    Reviewing the cases above they do not exposes weakness in left-to-right eval order and do not forces a named subquery as Mike suggested.
    The next one tries this pattern
    { 
      # [alpha] stuff with maybe ?X and maybe ?Y
      . . .
      {
          # additional stuff
         . . .
      } UNION {
          # [beta] more stuff with maybe ?X and maybe ?Y
         . . .
         MINUS {
             # [gamma] further stuff with maybe ?X and maybe ?Y
             . . .
         }
      }
   }
   the MINUS cannot be optimized away because of interaction between [beta] and [gamma], 
   whereas a left-to-right order will evaluate the minus after [alpha] which is incorrect.
   
    
   The issues are for any variable that 
   -  MAY be an incoming binding to the parent group
   -  is not a MUST incoming binding from the siblings
   -  and is a MAY binding within the MINUS

   A) Such variables might have a join involving that variable from the parent with the MINUS
   without the binding coming from the siblings (which define the bottom up semantics), this would
   remove solutions incorrectly

 */
    public void test_union_minus_11_bind() throws Exception {

        new Execute(
        		"SELECT  ?x ?y ?z                 \r\n" + 
        		"WHERE {                          \r\n" + 
        		"  BIND ( 3 as ?x )               \r\n" + 
        		"  BIND ( 4 as ?y )               \r\n" + 
        		"  {  }                           \r\n" + 
        		"  UNION                          \r\n" + 
        		"  { BIND ( 5 as ?z )             \r\n" + 
        		"    MINUS {                      \r\n" + 
        		"      { BIND ( 3 as ?x ) }       \r\n" + 
        		"      UNION                      \r\n" + 
        		"      { BIND ( 4 as ?z ) }       \r\n" + 
        		"    }                            \r\n" + 
        		"  }                              \r\n" + 
        		"}").expectResultSet("?x ?y ?z","3 4 UNDEF", "3 4 5");
        
    }
    public void test_union_minus_11_spo() throws Exception {

        new Execute(
        		"SELECT  ?x ?y ?z                 \r\n" + 
        		"WHERE {                          \r\n" + 
        		"  :a  :b  ?x     .               \r\n" + 
        		"  :a  :c  ?y     .               \r\n" + 
        		"  {  }                           \r\n" + 
        		"  UNION                          \r\n" + 
        		"  { :a  :d  ?z     .             \r\n" + 
        		"    MINUS {                      \r\n" + 
        		"      { :a  :b  ?x     . }       \r\n" + 
        		"      UNION                      \r\n" + 
        		"      { :a  :c  ?z     . }       \r\n" + 
        		"    }                            \r\n" + 
        		"  }                              \r\n" + 
        		"}").expectResultSet("?x ?y ?z","'ab' 'ac' UNDEF", "'ab' 'ac' 'ad'");
        
    }
    /*
     
    
   The issues are for any variable that 
   -  MAY be an incoming binding to the parent group
   -  is not a MUST incoming binding from the siblings
   -  and is a MAY binding within the MINUS
   B) Such variables may prevent a join between the parent bindings and the minus bindings
   which is possible between the sibling bindings and the minus bindings, hence incorrectly failing 
   to remove solutions 
     */
    public void test_union_minus_12_bind() throws Exception {

        new Execute(
        		"SELECT  ?x ?y ?z                 \r\n" + 
        		"WHERE {                          \r\n" + 
        		"  BIND ( 3 as ?x )               \r\n" + 
        		"  BIND ( 4 as ?y )               \r\n" + 
        		"  {  }                           \r\n" + 
        		"  UNION                          \r\n" + 
        		"  { BIND ( 5 as ?z )             \r\n" + 
        		"    MINUS {                      \r\n" + 
        		"         BIND ( 5 as ?z )        \r\n" + 
        		"         BIND ( 2 as ?y )        \r\n" + 
        		"    }                            \r\n" + 
        		"  }                              \r\n" + 
        		"}").expectResultSet("?x ?y ?z","3 4 UNDEF");
        
    }
    public void test_union_minus_12_spo() throws Exception {

        new Execute(
        		"SELECT  ?x ?y ?z                 \r\n" + 
        		"WHERE {                          \r\n" + 
        		"  :a  :b  ?x     .               \r\n" + 
        		"  :a  :c  ?y     .               \r\n" + 
        		"  {  }                           \r\n" + 
        		"  UNION                          \r\n" + 
        		"  { :a  :d  ?z     .             \r\n" + 
        		"    MINUS {                      \r\n" + 
        		"         :a  :d  ?z     .        \r\n" + 
        		"         :a  :b  ?y     .        \r\n" + 
        		"    }                            \r\n" + 
        		"  }                              \r\n" + 
        		"}").expectResultSet("?x ?y ?z","'ab' 'ac' UNDEF");
        
    }

    public void test_union_minus_double_nested_badly_designed_bind() throws Exception {

        new Execute(
        		"SELECT  ?x ?y ?z                 \r\n" + 
        		"WHERE {                          \r\n" + 
        		"  BIND ( 3 as ?x )               \r\n" + 
        		"  BIND ( 4 as ?y )               \r\n" + 
        		"  MINUS                          \r\n" + 
        		"  { BIND ( 5 as ?z )             \r\n" + 
        		"    BIND ( 3 as ?x )             \r\n" + 
        		"    MINUS {                      \r\n" + 
        		"         BIND ( 5 as ?z )        \r\n" + 
        		"         BIND ( 2 as ?y )        \r\n" + 
        		"    }                            \r\n" + 
        		"  }                              \r\n" + 
        		"}").expectResultSet("?x ?y ?z","3 4 UNDEF");
        
    }

    public void test_union_minus_double_nested_badly_designed_spo() throws Exception {

        new Execute(
        		"SELECT  ?x ?y ?z                 \r\n" + 
        		"WHERE {                          \r\n" + 
        		"  :a  :b  ?x     .               \r\n" + 
        		"  :a  :c  ?y     .               \r\n" + 
        		"  MINUS                          \r\n" + 
        		"  { :a  :d  ?z     .             \r\n" + 
        		"    :a  :b  ?x     .             \r\n" + 
        		"    MINUS {                      \r\n" + 
        		"         :a  :d  ?z     .        \r\n" + 
        		"         :a  :b  ?y     .        \r\n" + 
        		"    }                            \r\n" + 
        		"  }                              \r\n" + 
        		"}").expectResultSet("?x ?y ?z","'ab' 'ac' UNDEF");
        
    }
    /*
     * If there are maybe bound incoming sibling variables
     * that may be but not definitely be bound in the minus
     * then we have a potential problem, that can be addressed
     * by pulling the minus itself out as a named subquery
     * (not the parent join group) 
     */
    public void test_union_minus_13_bind() throws Exception {

        new Execute(
        		"SELECT  ?z                       \r\n" + 
        		"WHERE {                          \r\n" + 
        		"  BIND ( 5 as ?z )               \r\n" + 
        		"  MINUS {                        \r\n" + 
        		"      { BIND ( 3 as ?x ) }       \r\n" + 
        		"      UNION                      \r\n" + 
        		"      { BIND ( 4 as ?z ) }       \r\n" + 
        		"  }                              \r\n" + 
        		"}").expectResultSet("?z","5");
        
    }
    public void test_union_minus_minus_bind() throws Exception {

        new Execute(
        		"SELECT  ?x ?y                    \r\n" + 
        		"WHERE {                          \r\n" + 
        		"  { BIND ( 3 as ?x ) }           \r\n" + 
        		"  UNION                          \r\n" + 
        		"  { BIND ( 4 as ?y ) }           \r\n" + 
        		"  MINUS {                        \r\n" + 
        		"      BIND ( 3 as ?x )           \r\n" + 
        		"  }                              \r\n" + 
        		"  MINUS {                        \r\n" + 
        		"      BIND ( 4 as ?y )           \r\n" + 
        		"  }                              \r\n" + 
        		"}").expectResultSet("?x ?y");
        
    }
    public void test_union_nested_minus_minus_bind() throws Exception {

        new Execute(
        		"SELECT  ?x ?y ?z                 \r\n" + 
        		"WHERE {                          \r\n" + 
        		"  BIND ( 5 as ?z )               \r\n" + 
        		"  {                              \r\n" + 
        		"    { BIND ( 3 as ?x ) }         \r\n" + 
        		"    UNION                        \r\n" + 
        		"    { BIND ( 4 as ?y ) }         \r\n" + 
        		"    MINUS {                      \r\n" + 
        		"        BIND ( 3 as ?x )         \r\n" + 
        		"        BIND ( 2 as ?z )         \r\n" + 
        		"    }                            \r\n" + 
        		"    MINUS {                      \r\n" + 
        		"        BIND ( 4 as ?y )         \r\n" + 
        		"        BIND ( 5 as ?z )         \r\n" + 
        		"    }                            \r\n" + 
        		"  }                              \r\n" + 
        		"}").expectResultSet("?x ?y ?z");
        
    }
}
