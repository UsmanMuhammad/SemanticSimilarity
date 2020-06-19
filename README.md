# SemanticSimilarity
Semantic Similarity method to find similarity between RDF datasets in the form of triples for SANSA Stack using Scala and Spark

# Spark Submit Job
$SPARK_HOME/bin/spark-submit \
--class net.sansa_stack.template.spark.rdf.Semantic \
--master local[*] \
target/SANSA-Template-Maven-Spark-0.6.1-SNAPSHOT-jar-with-dependencies.jar \
--input new.nt \
--output /src/main/resources/output/
