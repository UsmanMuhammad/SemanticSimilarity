package net.sansa_stack.template.spark.rdf

import scala.collection.mutable
import scala.math.pow

import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import net.sansa_stack.query.spark.query._
import org.apache.jena.graph.NodeFactory
import org.apache.jena.graph.Node
import org.apache.jena.riot.Lang
import org.apache.spark.graphx
import org.apache.jena.query.Query
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{ Graph, VertexId, Edge }
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.util.Random.nextInt
import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.rdf.spark.partition._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.sparql.expr.NodeValue
import org.apache.jena.vocabulary.{ OWL, RDF, RDFS, XSD }
import org.apache.jena.sparql.function.library.Math_log
import org.apache.spark.mllib.rdd.RDDFunctions._
import scala.collection.mutable.ListBuffer

object TripleReader {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"Semantic Similarity  $input")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println(" | Similarity Measure - Wpath method |")
    println("======================================")

    val lang = Lang.NTRIPLES
    //val lang = Lang.TURTLE

    val triples = spark.rdf(lang)(input)


    //convert the RDD into a graph
    val graph = triples.asGraph()
    var k = 0.5;
    val numOfVertices = graph.vertices.count.toInt


    val sourceVertex = graph.vertices.first

    //Get the Object node to get the number of instances which is now important thing
    val newVertexId = graph.vertices.map(node => node._1)

    val list: List[Long] = newVertexId.collect().toList
    val listCount = list.length

    var distances: List[Long] = List()
    val sourceVertexId = list(0)
    val graph1: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(spark.sparkContext, numVertices = numOfVertices).mapEdges(e => e.attr.toDouble)

    val sourceId: VertexId = sourceVertexId // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    /*val initialGraph = graph1.mapVertices((id, _) =>
      if (id == sourceId) 0.0
      else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))*/

    //val distance = sssp.vertices.filter { case (destId, _) => destId == 1 }.first._2
    //println("The distance from the root to first edge is:" + distance)

    // Initialize the graph such that all vertices except the root have distance infinity.
    // works great to get a list. But we need a value.

    val initialGraph1: Graph[(Double, List[VertexId]), Double] = graph1.mapVertices((id, _) =>
      if (id == sourceId) (0.0, List[VertexId](sourceId))
      else (Double.PositiveInfinity, List[VertexId]()))

    val sssp1 = initialGraph1.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue)(

      // Vertex Program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

      // Send Message
      triplet => {
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },
      //Merge Message
      (a, b) => if (a._1 < b._1) a else b)

    println(sssp1.vertices.take(20).mkString("\n"))
    println("The loop starts from here:")

    var newRDD = sssp1.vertices.map(v => (sourceId, v._1, v._2))
    newRDD.take(20).foreach(println(_))

    val vlist: List[VertexId] = newVertexId.collect().toList

    for (w <- 1 until vlist.length) {
      var sourceId1: VertexId = vlist(w) // The ultimate source
      val initialGraph2: Graph[(Double, List[VertexId]), Double] = graph1.mapVertices((id, _) =>
        if (id == sourceId1) (0.0, List[VertexId](sourceId1))
        else (Double.PositiveInfinity, List[VertexId]()))

      val sssp2 = initialGraph2.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue)(

        // Vertex Program
        (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

        // Send Message
        triplet => {
          if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr) {
            Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcAttr._2 :+ triplet.dstId)))
          } else {
            Iterator.empty
          }
        },
        //Merge Message
        (a, b) => if (a._1 < b._1) a else b)

      var newRDD1 = sssp2.vertices.map(v => (sourceId1, v._1, v._2))
      newRDD = newRDD.union(newRDD1)

    }

    //newRDD.foreach(println(_))

    //This part calculate the Least Common Subsumer for all the node pairs.
    val result = sssp1.vertices.collect.toList

    var index = 0
    var resultList = result.length
    var lcsList = spark.sparkContext.emptyRDD[(VertexId, VertexId, VertexId)] //The first two parameters are nodes and the last one is the LCS.

    for (w <- 0 until (3)) {
      for (y <- w + 1 until 4) {
        var newNodePath1 = result(w)._2._2
        var newNodePath2 = result(y)._2._2

        var j = 0
        var lcs1: VertexId = newNodePath1(0)
        while (j < newNodePath1.length && j < newNodePath2.length) {
          if (newNodePath1(j) == newNodePath2(j)) {
            lcs1 = newNodePath1(j)
          }
          j = j + 1
        }
        var tempRDD = spark.sparkContext.parallelize(List((result(w)._1, result(y)._1, lcs1)))
        lcsList = lcsList.union(tempRDD)

      }
    }
    println("Lets see: ")

    lcsList.take(10).foreach(println(_))
    
    //Find IC of all the nodes.
    
    val totalInstances = triples
      .filter(triple => (triple.predicateMatches(RDF.`type`.asNode()) &&
        (triple.getSubject.isURI) &&
        (triple.objectMatches(OWL.Thing.asNode()))))
    val totalInstancesCount = totalInstances.count.toFloat
    val nodeList = graph.vertices.collect.toList
    var IC = spark.sparkContext.emptyRDD[(VertexId, Node, Float)]
    
    for (w <- 0 until numOfVertices) {

      var classInstances = triples
        .filter(triple => (triple.predicateMatches(RDF.`type`.asNode()) &&
          (triple.getSubject.isURI) &&
          (triple.objectMatches(nodeList(w)._2))))
          
      var classInstancesCount = classInstances.count.toFloat

      var ic = -(scala.math.log(classInstancesCount / totalInstancesCount)).toFloat
      
      var tempIC = spark.sparkContext.parallelize(List((nodeList(w)._1, nodeList(w)._2, ic)))
      IC = IC.union(tempIC)
    }
    
    println("Prints all IC of all the nodes")
    IC.take(20).foreach(println(_))
    
    //Find the Wpath Similarity measure for all the pair of nodes.
    /*var WPATH = spark.sparkContext.emptyRDD[(VertexId, VertexId, Double, Double)]
    for (w <- 0 until (3)) {
      for (y <- w + 1 until 4) {
        
        var wpath = 1.0 / (1.0 + (distance.toFloat * pow(k, ic).toFloat))
        
      }
    }*/
    
    val distance = sssp1.vertices.filter { case (destId, _) => destId == 1 }.first._2._1
    println("The distance from the root to first edge is:" + distance)

    //Find the least common subsumer
    val node1 = sssp1.vertices.filter { case (destId, _) => destId == 96 }.first._2
    val node2 = sssp1.vertices.filter { case (destId, _) => destId == 13 }.first._2
    println(node1)
    println(node2)
    val path1 = node1._2
    val path2 = node2._2

    //var length1 = path1.length - 1
    var i = 0
    var lcs: VertexId = path1(0)
    while (i < path1.length && i < path2.length) {
      if (path1(i) == path2(i)) {
        lcs = path1(i)
      }
      i = i + 1
    }

    println("The least common subsumer is:" + lcs)
    val LCSNode = graph.vertices.map(node => node._1).filter(f => f == lcs)
    //sssp1.vertices.foreach(println(_))

    //println(LCSNode)
    LCSNode.foreach(println(_));

    /* This part finds out the IC of concepts */
    /*val totalInstances = triples
      .filter(triple => (triple.predicateMatches(RDF.`type`.asNode()) &&
        (triple.getSubject.isURI) &&
        (triple.objectMatches(OWL.Thing.asNode()))))
    val totalInstancesCount = totalInstances.count.toFloat*/

    /*val classInstances = triples
      .filter(triple => (triple.predicateMatches(RDF.`type`.asNode()) &&
        (triple.getSubject.isURI) &&
        (triple.objectMatches(OWL.DatatypeProperty.asNode()))))*/

    val node = triples.getObjects.first()

    val sourceObject = graph.vertices.map(node => node._2).filter(f => f.toString() == "http://yago-knowledge.org/resource/wikicat_European_people")
    val test = sourceObject.first
    println(sourceObject.first)

    val classInstances = triples
      .filter(triple => (triple.predicateMatches(RDF.`type`.asNode()) &&
        (triple.getSubject.isURI) &&
        (triple.objectMatches(test))))
    val classInstancesCount = classInstances.count.toFloat

    println(node, classInstancesCount, totalInstancesCount)
    val ic = -(scala.math.log(classInstancesCount / totalInstancesCount)).toFloat
    println("Information Content of the LCS is:" + ic)

    val wpath = 1.0 / (1.0 + (distance.toFloat * pow(k, ic).toFloat))

    println("The wpath similarity score is:" + wpath)

    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Semantic Similarity") {

    head(" Semantic Similarity wpath method")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    help("help").text("prints this usage text")
  }
}