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
 * Created on Nov 17, 2006
 */
package com.bigdata.btree;

/**
 * A persistent object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class PO implements IIdentityAccess, IDirty {

    /**
     * The persistent identity (defined when the object is actually
     * persisted).
     */
    transient protected long identity = NULL;

    /**
     * True iff the object is deleted.
     */
    transient protected boolean deleted = false;
    
    final public boolean isPersistent() {

        return identity != NULL;

    }

    final public boolean isDeleted() {
        
        return deleted;
        
    }
    
    final public long getIdentity() throws IllegalStateException {

        if (identity == NULL) {
            
            throw new IllegalStateException();

        }

        return identity;

    }

    /**
     * Used by the store to set the persistent identity.
     * 
     * Note: This method should not be public.
     * 
     * @param identity
     *            The identity.
     * 
     * @throws IllegalStateException
     *             If the identity is already defined.
     */
    void setIdentity(long key) throws IllegalStateException {

        if (key == NULL) {

            throw new IllegalArgumentException();
            
        }

        if (this.identity != NULL) {

            throw new IllegalStateException("Object already persistent");
            
        }
        
        if( this.deleted) {
            
            throw new IllegalStateException("Object is deleted");
            
        }

        this.identity = key;

    }

    /**
     * New objects are considered to be dirty. When an object is
     * deserialized from the store the dirty flag MUST be explicitly
     * cleared.
     */
    transient protected boolean dirty = true;

    final public boolean isDirty() {

        return dirty;

    }

    public void setDirty(boolean dirty) {

        this.dirty = dirty;

    }

    /**
     * Extends the basic behavior to display the persistent identity of the
     * object iff the object is persistent and to mark objects that have been
     * deleted.
     */
    public String toString() {

        final String s;
        
        if (identity != NULL) {

            s = super.toString() + "#" + identity;

        } else {

            s = super.toString();

        }

        if (deleted) {

            return s + "(deleted)";

        } else {

            return s;

        }

    }

}
