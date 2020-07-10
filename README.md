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

./spark-submit --class net.sansa_stack.template.spark.rdf.Semantic --master spark://172.18.160.16:3090 hdfs://172.18.160.17:54310/MuhammadUsman/app/app-small.jar --input hdfs://172.18.160.17:54310/MuhammadUsman/input/new.nt --output hdfs://172.18.160.17:54310/MuhammadUsman/output --constant 0.5

# Commands for Hadoop and Spark

# Server 4
ssh -J MuhammadUsman@jump.aksw.uni-leipzig.de MuhammadUsman@akswnc4.aksw.uni-leipzig.de
## LocalHost
ssh -J MuhammadUsman@jump.aksw.uni-leipzig.de MuhammadUsman@akswnc4.aksw.uni-leipzig.de -L 4040:localhost:4040

# Server 5
ssh -J MuhammadUsman@jump.aksw.uni-leipzig.de MuhammadUsman@akswnc5.aksw.uni-leipzig.de
## LocalHost
ssh -J MuhammadUsman@jump.aksw.uni-leipzig.de MuhammadUsman@akswnc5.aksw.uni-leipzig.de -L 50070:localhost:50070

# SFTP
 sftp -o proxyjump=MuhammadUsman@jump.aksw.uni-leipzig.de MuhammadUsman@akswnc5.aksw.uni-leipzig.de

$ hadoop fs -mkdir /YourName/FolderName
$ hadoop fs -put /data/home/YourName/FileName   /YourName/FolderName

