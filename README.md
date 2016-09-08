# SeBiDA
Hi, here you find the source code of SeBiDA. It's actually a partial code, it shows only Spark code for schema extraction and loading of semantic (rdf) data (in nt format), which is for those having basic Spark knowledge easy to get started with. We will however publish a more complete code and make more detailed setup description for users with no prior knowledge in Spark.

In SchemExtration class, make the necessary changes, like:
	typeDataFrame.write().parquet()
to specify the write settings, host, port, etc.

Loading non-semantic data in SeBiDA is done the normal way you can find online: reading file to dataframe, creating schema if needed, or otherwise just save it back into a Parquet file. This latter requires third-party libraries for reading e.g., CSV and XML, which can easily be found online. Again, more detailed get-started steps for Spark beginners will flow later.

For benchmarking, we used BSBM benchmark data generator (http://wifo5-03.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/spec/BenchmarkRules/#datagenerator) and its 12 queries on SQL. For these latter, we actually didn't use the exact all SQL queries but rather created our own SQL conversion from BSBM SPARQL queries. This is because of the limited syntax of Spark SQL comparing to the standard SQL syntax used in BSBM standard. The rewritten SQL queries will also published soon.

For more information, please contact me at: mami@cs.uni-bonn.de, and I'll be happy to assist.
