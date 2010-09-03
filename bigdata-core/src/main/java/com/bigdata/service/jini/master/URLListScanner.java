/*

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

package com.bigdata.service.jini.master;

import com.bigdata.relation.accesspath.BlockingBuffer;
import java.io.IOException;
import java.net.URL;

/**
 * A resource factory providing a list of URLs for datafiles to be loaded
 * during a bulk load operation. This class is intended to be used from a
 * cluster configuration file.
 */
public class URLListScanner extends AbstractResourceScanner<URL> {

    private URL[] urlList;

    @Override
    public String toString() {
        return getClass() + "{acceptCount=" + super.getAcceptCount() + "}";
    }
    
    /**
     * Create a container for the list of URLs to be supplied for bulk loading.
     * @param buffer
     *            Chunks of files to be loaded are added to this buffer.
     * @param urlList the list of URLs for data files to bulk load.
     */
    public URLListScanner(final BlockingBuffer<URL[]> buffer,
                          final URL[] urlList) {
        super(buffer);
        if (urlList == null)
            throw new IllegalArgumentException();
        this.urlList = urlList;
    }

    @Override
    protected void runScanner() throws Exception {
        for (URL url : urlList) {
            accept(url);
        }
    }

    /**
     * Create the resource factory for the list of URLs to be fed to the
     * bulk loading mechanism. This method is intended to be called from
     * the cluster configuration file.
     * @param urlList the list of URLs to datafiles to bulk load.
     * @return the resource factory based on the list of URLs.
     */
    public static IResourceScannerFactory<URL> newFactory(final URL[] urlList) {

        return new IResourceScannerFactory<URL>() {
            private static final long serialVersionUID = 3L;

            public AbstractResourceScanner<URL> newScanner(
                    final BlockingBuffer<URL[]> buffer) throws IOException {
                return new URLListScanner(buffer, urlList);
            }
        };
    }
}
