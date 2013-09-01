package com.bigdata.rdf.graph.impl.sail;

import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.sail.Sail;

import com.bigdata.rdf.graph.AbstractGraphTestCase;
import com.bigdata.rdf.graph.util.IGraphFixture;
import com.bigdata.rdf.graph.util.IGraphFixtureFactory;

public class AbstractSailGraphTestCase extends AbstractGraphTestCase {

//    private static final Logger log = Logger
//            .getLogger(AbstractGraphTestCase.class);
    
    public AbstractSailGraphTestCase() {
    }

    public AbstractSailGraphTestCase(String name) {
        super(name);
    }

    @Override
    protected IGraphFixtureFactory getGraphFixtureFactory() {

        return new IGraphFixtureFactory() {

            @Override
            public IGraphFixture newGraphFixture() throws Exception {
                return new SailGraphFixture();
            }
            
        };

    }

    @Override
    protected SailGraphFixture getGraphFixture() {

        return (SailGraphFixture) super.getGraphFixture();

    }

    /**
     * A small foaf data set relating some of the project contributors (triples
     * mode data).
     * 
     * @see {@value #smallGraph}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    protected class SmallGraphProblem {

        /**
         * The data file.
         */
        static private final String smallGraph = "bigdata-rdf/src/test/com/bigdata/rdf/graph/data/smallGraph.ttl";
        
        private final URI rdfType, foafKnows, foafPerson, mike, bryan, martyn;

        public SmallGraphProblem() throws Exception {

            getGraphFixture().loadGraph(smallGraph);

            final Sail sail = getGraphFixture().getSail();

            rdfType = sail.getValueFactory().createURI(RDF.TYPE.stringValue());

            foafKnows = sail.getValueFactory().createURI(
                    "http://xmlns.com/foaf/0.1/knows");

            foafPerson = sail.getValueFactory().createURI(
                    "http://xmlns.com/foaf/0.1/Person");

            mike = sail.getValueFactory().createURI(
                    "http://www.bigdata.com/Mike");

            bryan = sail.getValueFactory().createURI(
                    "http://www.bigdata.com/Bryan");

            martyn = sail.getValueFactory().createURI(
                    "http://www.bigdata.com/Martyn");

        }

        public URI getRdfType() {
            return rdfType;
        }

        public URI getFoafKnows() {
            return foafKnows;
        }

        public URI getFoafPerson() {
            return foafPerson;
        }

        public URI getMike() {
            return mike;
        }

        public URI getBryan() {
            return bryan;
        }

        public URI getMartyn() {
            return martyn;
        }

    }

    /**
     * Load and setup the {@link SmallGraphProblem}.
     */
    protected SmallGraphProblem setupSmallGraphProblem() throws Exception {

        return new SmallGraphProblem();

    }

}
