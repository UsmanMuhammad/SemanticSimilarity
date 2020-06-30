# SemanticSimilarity
Semantic Similarity method to find similarity between RDF datasets in the form of triples for SANSA Stack using Scala and Spark

# Spark Submit Job
$SPARK_HOME/bin/spark-submit \
--class net.sansa_stack.template.spark.rdf.Semantic \
--master local[*] \
target/SANSA-Template-Maven-Spark-0.6.1-SNAPSHOT-jar-with-dependencies.jar \
--input new.nt \
--output /src/main/resources/output/

# On Server

./spark-submit --class net.sansa_stack.template.spark.rdf.Semantic --master spark://172.18.160.16:3090 hdfs://172.18.160.17:54310/MuhammadUsman/app/app.jar --input hdfs://172.18.160.17:54310/MuhammadUsman/input/new.nt --output hdfs://172.18.160.17:54310/MuhammadUsman/output/ --constant 0.5

