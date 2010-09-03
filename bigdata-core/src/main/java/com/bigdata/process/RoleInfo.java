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
package com.bigdata.process;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * Class used as a data structure consisting of information related to
 * the "role" of a single node in a federation. 
 *
 * Note that a node can have multiple roles.
 */
class RoleInfo {

    /**
     * The type, layout, and role described by the current instance of
     * this class.
     */
    private final ProcessInfo pInfo;

    /**
     * The set of processes that should be running on a node configured
     * with the role represented by the current instance of this class.
     */
    private final Set<String> processSet = new TreeSet<String>();


    public RoleInfo(ProcessInfo pInfo) {
        this.pInfo = pInfo;
    }

    /**
     * Returns the type, layout, and role represented by the current instance
     * of this class.
     */
    public ProcessInfo getProcessInfo() {
        return pInfo;
    }

    /**
     * Returns the node type represented by the current instance of this
     * class.
     */
    public String getType() {
        return pInfo.getType();
    }

    /**
     * Returns the process layout for the node represented by the current
     * instance of this class.
     */
    public String getLayout() {
        return pInfo.getLayout();
    }

    /**
     * Returns the name of the role described by the current instance of
     * this class.
     */
    public String getRole() {
        return pInfo.getRole();
    }

    /**
     * Adds a process to the set of processes that should be running on
     * a node configured with the type, layout, and role represented by
     * the current instance of this class.
     */
    public void addProcess(String pTag) {
        processSet.add(pTag);
    }

    /**
     * Returns the set of processes that should be running on a node
     * configured with the type, layout, and role represented by the
     * current instance of this class.
     */
    public Set<String> getProcessSet() {
        return Collections.unmodifiableSet(processSet);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if( !(obj instanceof RoleInfo) ) return false;

        if( obj == this ) return true;//same object

        RoleInfo obj1 = this;
        RoleInfo obj2 = (RoleInfo)obj;

        ProcessInfo pInfo1 = obj1.getProcessInfo();
        ProcessInfo pInfo2 = obj2.getProcessInfo();

        if( (pInfo1 == null) && (pInfo2 != null) ) return false;
        if( (pInfo1 != null) && (pInfo2 == null) ) return false;
        if( (pInfo1 == null) && (pInfo2 == null) ) {
            throw new AssertionError("RoleInfo.equals - pInfo "
                                     +"field is null in both objects");
        }
        if( !(pInfo1.equals(pInfo2)) ) return false;

        Set<String> pSet1 = obj1.getProcessSet();
        Set<String> pSet2 = obj2.getProcessSet();

        if( !(pSet1.containsAll(pSet2)) ) return false;
        if( !(pSet2.containsAll(pSet1)) ) return false;

        return true;
    }

    @Override
    public String toString() {
        return getProcessInfo()+":"+getProcessSet();
    }
}
