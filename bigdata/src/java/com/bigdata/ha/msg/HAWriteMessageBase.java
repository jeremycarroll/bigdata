/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.ha.msg;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Random;

import com.bigdata.btree.BytesUtil;
import com.bigdata.ha.pipeline.HAReceiveService;
import com.bigdata.ha.pipeline.HASendService;

/**
 * Base class for RMI messages used to communicate metadata about a raw data
 * transfer occurring on a socket channel.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see HASendService
 * @see HAReceiveService
 */
public class HAWriteMessageBase implements Externalizable, IHAWriteMessageBase {

	/**
     * 
     */
    private static final long serialVersionUID = -6807744466616574690L;

    /** The #of bytes of data to be transfered. */
    private int sze;

    /** The Alder32 checksum of the bytes to be transfered. */
    private int chk;

    /** A byte[] token that must prefix the message payload, needed to skip stale data from failed read tasks */
    private byte[] token;

    static private Random r = new Random();
    static final private int TOKEN_SIZE = 8;
    
    static private byte[] genToken() {
    	final byte[] token = new byte[TOKEN_SIZE];
    	
    	while (!unique1(token)) {
    		r.nextBytes(token);
    	}
    	
    	return token;
    }
    
    /**
     * Checks that the first byte is not repeated in the
     * remaining bytes, this simplifies search for the token
     * in the input stream.
     */
	static private boolean unique1(final byte[] bytes) {
		final byte b = bytes[0];
		for (int t = 1; t < bytes.length; t++) {
			if (bytes[t] == b)
				return false;
		}

		return true;
	}

    /**
     * 
     * @param sze
     *            The #of bytes of data to be transfered.
     * @param chk
     *            The Alder32 checksum of the bytes to be transfered.
     */
    public HAWriteMessageBase(final int sze, final int chk) {
    	this(sze, chk, genToken());
    }
    
    public HAWriteMessageBase(final int sze, final int chk, final byte[] token) {

        if (sze <= 0)
            throw new IllegalArgumentException();
		
        this.sze = sze;
		
        this.chk = chk;
        
        this.token = token;
        
	}
	
    /**
     * Deserialization constructor.
     */
	public HAWriteMessageBase() {}

	/* (non-Javadoc)
     * @see com.bigdata.ha.pipeline.IHAWriteMessageBase#getSize()
     */
	@Override
    public int getSize() {

	    return sze;
	    
	}

	/* (non-Javadoc)
     * @see com.bigdata.ha.pipeline.IHAWriteMessageBase#getChk()
     */
	@Override
    public int getChk() {

	    return chk;
	    
	}
	
    public String toString() {

        return super.toString() + "{size=" + sze + ",chksum=" + chk + "}";
        
    }
    
    @Override
    public boolean equals(final Object obj) {

        if (this == obj)
            return true;
        
        if (!(obj instanceof IHAWriteMessageBase))
            return false;
        
        final IHAWriteMessageBase t = (IHAWriteMessageBase) obj;

        return sze == t.getSize() && chk == t.getChk() && (BytesUtil.compareBytes(t.getToken(), getToken()) == 0);

    }

    @Override
    public int hashCode() {

        // checksum is a decent hash code if given otherwise the size.
        return chk == 0 ? sze : chk;

    }
    
    private static final transient short VERSION0 = 0x0;
    
    private static final transient short currentVersion = VERSION0;
    
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final short version = in.readShort();

        if (version != VERSION0)
            throw new RuntimeException("Bad version for serialization");

        sze = in.readInt();
        
        chk = in.readInt();
        
        // read token
        final int tlen = in.readInt();
        if (tlen == 0) {
        	token = null;
        } else {
        	token = new byte[tlen];
        	in.read(token);
        }
        
    }

    public void writeExternal(final ObjectOutput out) throws IOException {
        
        out.writeShort(currentVersion);
    	
        out.writeInt(sze);
        
        out.writeInt(chk);
        
        if (token == null) {
        	out.writeInt(0);
        } else {
        	out.writeInt(token.length);
        	out.write(token);
        }
    }

	@Override
	public byte[] getToken() {
		return token;
	}

}
