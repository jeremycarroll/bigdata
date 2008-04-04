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
package com.bigdata.rdf.store;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._Resource;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;

/**
 * Also reports whether the statement is explicit, inferred or an axiom.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatementWithType extends StatementImpl {

    /**
     * 
     */
    private static final long serialVersionUID = 6739949195958368365L;

    private StatementEnum type;
    
    /**
     * @param subject
     * @param predicate
     * @param object
     */
    public StatementWithType(Resource subject, URI predicate, Value object,StatementEnum type) {
        
        super(subject, predicate, object);
        
        this.type = type;
        
    }

    /**
     * Whether the statement is explicit, inferred or an axiom.
     */
    public StatementEnum getStatementType() {
        
        return type;
        
    }
 
}
