This directory contains a setup for running BSBM v3 against bigdata.  

In addition to the files in this directory, you will need the bsbmtools
distribution.  This is available from
http://www4.wiwiss.fu-berlin.de/bizer/BerlinSPARQLBenchmark.  Please consult
bsbmtools and the online documentation for BSBM for current information on
how to generate test data sets and the correct procedure for running the
benchmark. 

The files in this directory include:

- build.properties - configuration properties for the ant script.

- build.xml - an ant script which may be used to load a generated data set 
              a local bigdata database instance and start a SPARQL 
              end point for that database instance.  You will then run the
              benchmark against that SPARQL end point.

- RWStore.properties - configuration properties for a bigdata database instance
                       suitable for BSBM and backed by the RW persistence engine
                       (single machine read/write bigdata database). This is the
                       recommended database mode for BSBM as it has significantly
                       better performance.
                       
- WORMStore.properties - configuration properties for a bigdata database instance
                         suitable for BSBM and backed by the WORM persistence
                         engine (single machine write once, read many bigdata
                         database).

Other requirements include:

- A 64-bit OS and a 64-bit server JVM.  We have tested most extensible with Oracle
  JDK 1.6.0_17.

- Apache ant (version 1.8.0+).
  
- Bigdata (check it out from SVN).
                         
To get started:

0. Generate a suitable data set.

2. Edit bigdata-perf/bsbm3/build.properties.

3. In the top-level directory of the bigdata source tree, review build.properties
   and then do:
   
   a. "ant bundleJar".
   
   Note: You will need to rerun this ant target any time you update the code
   from SVN or if you make edits to the source tree.
   
4. Change to the bigdata-perf/bsbm3 directory:

   b. "ant run-load" (loads the generated data set into a bigdata instance).
   
   c. "ant start-nano-server" (starts the SPARQL end point).

5. Follow the procedure for BSBM tools to run the benchmark against the SPARQL
   end point.

Performance should be extremely good for the reduced query mix, which can be
enabled by editing:

  bigdata-perf/bsbm3/bsbmtools/queries/explore/ignoreQueries
  
For the reduced query mix, "ignoreQueries" should contain "5 6". For the full
query mix, it should be an empty file (the reduced query mix is enabled by
default in SVN).

Notes on the queries:

The static query optimizer and vectored pipelined joins do a great job on most
of the BSBM queries.  However, there are two queries which do not do so well out
of the box:

Query 5 has a bad join plan using the static query optimizer.  Good performance
for query 5 can be achieved by replacing the contents of:

   bigdata-perf/bsbm3/bsbmtools/queries/explore/query5.txt

   bigdata-perf/bsbm3/bsbmtools/queries/explore/query5-explicit-order.txt

The original version of query5 has also been saved as query5-original.txt

Query 6 is uses a REGEX filter.  Bigdata does not have index support for REGEX,
so this winds up visiting a lot of data and then filtering using the REGEX. This
drags the overall performance down dramatically.  It is possible to integrate
bigdata with Lucene, which does support indexed regular expressions, but that is
not something which works out of the box.
