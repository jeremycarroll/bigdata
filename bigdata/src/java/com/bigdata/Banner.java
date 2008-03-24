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
/*
 * Created on Mar 24, 2008
 */

package com.bigdata;

/**
 * Class has a static method which writes a copyright banner on stdout once per
 * JVM. This method is invoked from several core classes in order to ensure that
 * the copyright banner is always written out on bigdata startup.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Banner {

    private static boolean didBanner;
    
    synchronized static public void banner() {
        
        if(!didBanner) {
        
            System.out.println(banner);
         
            didBanner = true;
            
        }
        
    }

    private static final String banner =
        "\nBIGDATA(tm)"+
        "\n"+
        "\n                   Flexible"+
        "\n                   Reliable"+
        "\n                  Affordable"+
        "\n      Web-Scale Computing for the Enterprise"+
        "\n"+
        "\nCopyright SYSTAP, LLC 2006-2008.  All rights reserved."+
        "\n"
        ;
    
}
