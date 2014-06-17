/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.rdf.sail;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.axioms.OwlAxioms;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;
import com.bigdata.rdf.vocab.RDFSVocabulary;

/**
 * Helper class to create a bigdata instance.
 * 
 * @author mikepersonick
 *
 */
public class BigdataSailFactory {

    /**
     * A handy list of common Options you might want to specify when creating
     * your bigdata instance.
     * 
     * @author mikepersonick
     *
     */
    public static enum Option {
        
        /**
         * Inference on or off.  Off by default.
         */
        Inference,
        
        /**
         * Quads on or off.  Off by default.
         */
        Quads,
        
        /**
         * RDR (statement identifiers) on or off.  Off by default.
         */
        RDR,
        
        /**
         * Text index on or off.  Off by default.
         */
        TextIndex,
        
//        /**
//         * Create an in-memory instance.
//         */
//        InMemory,
//        
//        /**
//         * Create a persistent instance backed by a file.  You must specify
//         * the file.
//         */
//        Persistent
        
    }
    
    /**
     * Connect to a remote bigdata instance.
     */
    public static BigdataSailRemoteRepository connect(final String host, final int port) {
        
        return connect("http://"+host+":"+port);
        
    }
    
    /**
     * Connect to a remote bigdata instance.
     * 
     * FIXME This does not support the HA load balancer pattern and does not
     * parameterize the value of the ContextPath. Also, should this class be
     * part of the "client" package?
     */
    public static BigdataSailRemoteRepository connect(final String serviceEndpoint) {
        
        if (serviceEndpoint.endsWith("/bigdata/sparql")) {
            return new BigdataSailRemoteRepository(serviceEndpoint);
        } else if (serviceEndpoint.endsWith("/bigdata/")) {
            return new BigdataSailRemoteRepository(serviceEndpoint + "sparql");
        } else if (serviceEndpoint.endsWith("/bigdata")) {
            return new BigdataSailRemoteRepository(serviceEndpoint + "/sparql");
        } else if (serviceEndpoint.endsWith("/")) {
            return new BigdataSailRemoteRepository(serviceEndpoint + "bigdata/sparql");
        } else {
            return new BigdataSailRemoteRepository(serviceEndpoint + "/bigdata/sparql");
        }
        
    }

    /**
     * Open an existing persistent bigdata instance.
     */
    public static BigdataSailRepository openRepository(final String file) {
        
        return new BigdataSailRepository(openSail(file));
        
    }
        
    /**
     * Open an existing persistent bigdata instance.
     */
    public static BigdataSail openSail(final String file) {
        
        if (!new File(file).exists()) {
            throw new IllegalArgumentException("file does not exist - use create() method instead");
        }
        
        final Properties props = new Properties();
        props.setProperty(BigdataSail.Options.FILE, file);
        
        final BigdataSail sail = new BigdataSail(props);
        
        return sail;
        
    }
    
    /**
     * Create a new bigdata instance using the specified options.  Since no
     * journal file is specified this must be an in-memory instance.
     */
    public static BigdataSailRepository createRepository(final Option... args) {
        
        return new BigdataSailRepository(createSail(null, args));
        
    }
    
    /**
     * Create a new bigdata instance using the specified options.
     */
    public static BigdataSailRepository createRepository(final String file,
            final Option... args) {
        
        return new BigdataSailRepository(createSail(file, args));
        
    }
    
    /**
     * Create a new bigdata instance using the specified options.  Since no
     * journal file is specified this must be an in-memory instance.
     */
    public static BigdataSail createSail(final Option... args) {
        
        return createSail(null, args);
        
    }
    
    /**
     * Create a new bigdata instance using the specified options.
     */
    public static BigdataSail createSail(final String file,
            final Option... args) {
        
        final List<Option> options = args != null ? 
                Arrays.asList(args) : new LinkedList<Option>();

        checkArgs(file, options);
                
        final Properties props = new Properties();
        
        if (file != null) {
            props.setProperty(BigdataSail.Options.FILE, file);
            props.setProperty(Journal.Options.BUFFER_MODE, BufferMode.DiskRW.toString());
        } else {
            props.setProperty(Journal.Options.BUFFER_MODE, BufferMode.MemStore.toString());
        }
        
        if (options.contains(Option.Inference)) {
            props.setProperty(BigdataSail.Options.AXIOMS_CLASS, OwlAxioms.class.getName());
            props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, RDFSVocabulary.class.getName());
            props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "true");
            props.setProperty(BigdataSail.Options.JUSTIFY, "true");
        } else {
            props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
            props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, RDFSVocabulary.class.getName());
            props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
            props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        }
        
        props.setProperty(BigdataSail.Options.TEXT_INDEX, 
                String.valueOf(options.contains(Option.TextIndex)));
        
        props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, 
                String.valueOf(options.contains(Option.RDR)));
        
        props.setProperty(BigdataSail.Options.QUADS, 
                String.valueOf(options.contains(Option.Quads)));
        
        // Setup for the RWStore recycler rather than session protection.
        props.setProperty("com.bigdata.service.AbstractTransactionService.minReleaseAge","1");
        props.setProperty("com.bigdata.btree.writeRetentionQueue.capacity","4000");
        props.setProperty("com.bigdata.btree.BTree.branchingFactor","128");
        // Bump up the branching factor for the lexicon indices on the default kb.
        props.setProperty("com.bigdata.namespace.kb.lex.com.bigdata.btree.BTree.branchingFactor","400");
        // Bump up the branching factor for the statement indices on the default kb.
        props.setProperty("com.bigdata.namespace.kb.spo.com.bigdata.btree.BTree.branchingFactor","1024");
        
        final BigdataSail sail = new BigdataSail(props);
        
        return sail;
        
    }
    
    protected static void checkArgs(final String file, final List<Option> options) {
        
        if (options.contains(Option.Inference) && options.contains(Option.Quads)) {
            throw new IllegalArgumentException();
        }
        
        if (options.contains(Option.RDR) && options.contains(Option.Quads)) {
            throw new IllegalArgumentException();
        }
        
//        if (options.contains(Option.InMemory) && options.contains(Option.Persistent)) {
//            throw new IllegalArgumentException();
//        }
//        
//        if (options.contains(Option.InMemory) && file != null) {
//            throw new IllegalArgumentException();
//        }
//        
//        if (options.contains(Option.Persistent) && file == null) {
//            throw new IllegalArgumentException();
//        }
        
    }
    
    
}
