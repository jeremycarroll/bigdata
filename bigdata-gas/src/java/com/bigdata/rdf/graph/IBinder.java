/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

/**
 * An interface that may be used to extract variable bindings for the
 * vertices visited by the algorithm.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public interface IBinder<VS, ES, ST> {

    /**
     * The ordinal index of the variable that is bound by this
     * {@link IBinder}. By convention, index ZERO is the vertex. Indices
     * greater than ZERO are typically aspects of the state of the vertex.
     */
    int getIndex();

    /**
     * @param vf
     *            The {@link ValueFactory} used to create the return
     *            {@link Value}.
     * @param u
     *            The vertex.
     * 
     * @return The {@link Value} for that ordinal variable or
     *         <code>null</code> if there is no binding for that ordinal
     *         variable.
     */
    Value bind(ValueFactory vf, final IGASState<VS, ES, ST> state, Value u);

}
