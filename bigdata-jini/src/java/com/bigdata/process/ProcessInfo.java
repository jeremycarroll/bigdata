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

/**
 * Class used as a data structure consisting of information related to
 * the "type", "layout", and "role" of a single node in a federation. 
 *
 * Note that a node can have multiple types, layouts, and roles.
 */
class ProcessInfo implements Comparable<ProcessInfo> {

    /** The type of node the current instance class describes. */
    private final String type;

    /** The node's process layout described by the current instance class. */
    private final String layout;

    /** The name of the role the current instance class describes. */
    private final String role;

    public ProcessInfo(String type, String layout, String role) {
        this.type   = type;
        this.layout = layout;
        this.role   = role;
    }

    /**
     * Returns the type of node represented by the current instance
     * of this class.
     */
    public String getType() {
        return type;
    }

    /**
     * Returns the process layout for the node represented by the current
     * instance of this class.
     */
    public String getLayout() {
        return layout;
    }

    /**
     * Returns the name of the role represented by the current instance
     * of this class.
     */
    public String getRole() {
        return role;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if( !(obj instanceof ProcessInfo) ) return false;

        if( obj == this ) return true;//same object

        ProcessInfo obj1 = this;
        ProcessInfo obj2 = (ProcessInfo)obj;

        String type1 = obj1.getType();
        String type2 = obj2.getType();
        if( (type1 == null) && (type2 != null) ) return false;
        if( (type1 != null) && (type2 == null) ) return false;
        if( (type1 == null) && (type2 == null) ) {
            throw new AssertionError("ProcessInfo.equals - type "
                                     +"field is null in both objects");
        }
        if( !(type1.equals(type2)) ) return false;

        String layout1 = obj1.getLayout();
        String layout2 = obj2.getLayout();
        if( (layout1 == null) && (layout2 != null) ) return false;
        if( (layout1 != null) && (layout2 == null) ) return false;
        if( (layout1 == null) && (layout2 == null) ) {
            throw new AssertionError("ProcessInfo.equals - layout "
                                     +"field is null in both objects");
        }
        if( !(layout1.equals(layout2)) ) return false;

        String role1 = obj1.getRole();
        String role2 = obj2.getRole();
        if( (role1 == null) && (role2 != null) ) return false;
        if( (role1 != null) && (role2 == null) ) return false;
        if( (role1 == null) && (role2 == null) ) {
            throw new AssertionError("ProcessInfo.equals - role "
                                     +"field is null in both objects");
        }
        if( !(role1.equals(role2)) ) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int hc = 11;//prime initial value
        int pm = 37;//prime multiplier
        if(type   != null) hc = (pm*hc) + type.hashCode();
        if(layout != null) hc = (pm*hc) + layout.hashCode();
        if(role   != null) hc = (pm*hc) + role.hashCode();
        return hc;
    }

    @Override
    public String toString() {
        return "[type="+type+", layout="+layout+", role="+role+"]";
    }

    @Override // java.lang.Comparable
    public int compareTo(ProcessInfo other) {
        if(other == null) throw new NullPointerException("null input");

        // Order by type, then layout, then finally role

        String type1 = this.getType();
        String type2 = other.getType();
        if(type1 != null) {
            if(type2 != null) {
                int typeComparison = type1.compareTo(type2);
                if(typeComparison != 0) return typeComparison;
            } else {
                return 1;//type1 > type2
            }
        } else {
            if(type2 != null) {//type1 == null && type2 != null
                return -1;//type1 < type2
            }
        }

        String layout1 = this.getLayout();
        String layout2 = other.getLayout();
        if(layout1 != null) {
            if(layout2 != null) {
                int layoutComparison = layout1.compareTo(layout2);
                if(layoutComparison != 0) return layoutComparison;
            } else {
                return 1;//layout1 > layout2
            }
        } else {
            if(layout2 != null) {
                return -1;//layout1 < layout2
            }
        }

        String role1 = this.getRole();
        String role2 = other.getRole();
        if(role1 != null) {
            if(role2 != null) {
                int roleComparison = role1.compareTo(role2);
                if(roleComparison != 0) return roleComparison;
            } else {
                return 1;//role1 > role2
            }
        } else {
            if(role2 != null) {
                return -1;//role1 < role2
            }
        }
        return 0;//all fields null
    }
}
