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
package com.bigdata.boot;

import java.io.Serializable;

public class ProcessStateChangeEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The identifier ('tag') corresponding to the process that encountered
     *  a change in its state.
     */
    public final String tag;

    /** The previous state of the process. */
    public final ProcessState prevState;

    /** The current state of the process. */
    public final ProcessState currState;

    public ProcessStateChangeEvent(String       tag,
                                   ProcessState prev,
                                   ProcessState curr)
    {
        this.tag = tag;
        this.prevState = prev;
        this.currState = curr;
    }
}
