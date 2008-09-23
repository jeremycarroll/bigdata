/*

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
package com.bigdata.relation.rule;

/**
 * Imposes the constraint <code>x OR y</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OR implements IConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = -1L;
    
    private final IConstraint x;
    private final IConstraint y;
    
    public OR(IConstraint x, IConstraint y) {
        
        if (x == null || y == null)
            throw new IllegalArgumentException();

        if (x == y)
            throw new IllegalArgumentException();
        
        this.x = x;
        
        this.y = y;
        
    }
    
    public boolean accept(IBindingSet s) {

        return x.accept(s) || y.accept(s);

   }

}
