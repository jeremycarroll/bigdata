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

public class InitializationException extends Exception {

    private static final long serialVersionUID = 1L;

    /** Constructs an instance of this exception with no detail message. */
    public InitializationException() { }

    /**
     * Constructs an instance of this exception with the given detail
     * <code>message</code>.
     */
    public InitializationException(String message) {
        super(message);
    }

    /**
     * Constructs an instance of this exception with the given detail
     * <code>message</code> and the given <code>cause</code>.
     */
    public InitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an instance of this exception with the given
     * <code>cause</code>, and with a detail message derived from
     * that given cause.
     */
    public InitializationException(Throwable cause) {
        super(cause);
    }
}
