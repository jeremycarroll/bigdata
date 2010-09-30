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
package com.bigdata.util;

import java.text.MessageFormat;

/**
 * Convenience class that can be used to format log messages such that
 * the relatively expensive string formatting only occurs when the
 * logger actually logs the desired message. To achieve this so-called
 * "lazy formatting", this class is instantiated with a format string
 * and its arguments. The string is then formatted only when the
 * instantiated object's <code>toString</code> method is invoked by
 * the logger; which occurs only when the log level satisfies the
 * necessary criteria for the message to be logged.
 */
//TODO - replace references with log level checks intstead
public class Format {

    private final String pattern;
    private final Object[] arguments;

    /**
     * Constructs an instance of this class with the given format
     * <code>String</code> and arguments. The value input for the
     * <code>pattern</code> parameter must be valid, as specified
     * by the <code>java.text.MessageFormat</code> class.
     */
    public Format(String pattern, Object... arguments) {
        this.pattern   = pattern;
        this.arguments = arguments;
    }

    @Override
    public String toString() {
        return MessageFormat.format(pattern, arguments);
    }
}
