/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 15, 2006
 */
package com.bigdata.btree;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import com.bigdata.btree.DataOutputBuffer;
import com.bigdata.btree.IValueSerializer;
import com.bigdata.btree.Leaf;


/**
 * Test helper provides an entry (aka value) for a {@link Leaf}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleEntry {

    private static int nextId = 1;

    private final int id;
    
    /**
     * Create a new entry.
     */
    public SimpleEntry() {
        
        id = nextId++;
        
    }

    public SimpleEntry(int id){
    
        this.id = id;
        
    }
    
    public int id() {
        
        return id;
        
    }
    
    public String toString() {
        
        return ""+id;
        
    }

    public boolean equals(Object o) {
        
        if( this == o ) return true;
        
        if( o == null ) return false;
        
        return id == ((SimpleEntry)o).id;
        
    }
    
    
    /**
     * (De-)serializer an array of {@link SimpleEntry}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Serializer implements IValueSerializer {

        /**
         * 
         */
        private static final long serialVersionUID = 4515322522558633041L;
        
        public transient static final Serializer INSTANCE = new Serializer();
        
        public Serializer() {}

        public void putValues(DataOutputBuffer os, Object[] values, int n)
                throws IOException {

            for (int i = 0; i < n; i++) {

                os.writeInt(((SimpleEntry) values[i]).id);

            }

        }

        public void getValues(DataInput is, Object[] values, int n)
                throws IOException {

            for (int i = 0; i < n; i++) {

                values[i] = new SimpleEntry(is.readInt());

            }

        }

    }

    /**
     * A (De-)serializer that always throws exceptions.  This is used when we
     * are testing in a context in which incremental IOs are disabled, e.g.,
     * by the {@link NoEvictionListener}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class NoSerializer implements IValueSerializer {

        /**
         * 
         */
        private static final long serialVersionUID = -6467578720380911380L;
        
        public transient static final NoSerializer INSTANCE = new NoSerializer();
        
        public NoSerializer() {}
        
        public void getValues(DataInput is, Object[] values, int n) throws IOException {

            throw new UnsupportedOperationException();
 
        }

        public void putValues(DataOutputBuffer os, Object[] values, int n) throws IOException {

            throw new UnsupportedOperationException();

        }

    }

}
