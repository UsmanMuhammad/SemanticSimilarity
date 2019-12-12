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
import org.apache.spark.graphx.{ Graph, VertexId, Edge, EdgeDirection }
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
import scala.collection.mutable.Stack

object Semantic {

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

    //Data in triples
    val triples = spark.rdf(lang)(input)

    //convert the RDD into a graph
    val graph = buildGraph(triples)

    val root = graph.vertices.first

    var k = 0.5; //Value of k that we use in wpath method.
    val numOfVertices = graph.vertices.count.toInt

    //Get only the VertexIds of all the nodes.
    val VertexIds: List[VertexId] = graph.vertices.map(node => node._1).collect().toList
    

    val VertexIdsCount = VertexIds.length
    val wpathList = initialList(VertexIds, VertexIdsCount)
    

    val totalInstances = triples
      .filter(triple => (triple.predicateMatches(RDF.`type`.asNode()) &&
        (triple.getSubject.isURI) &&
        (triple.objectMatches(OWL.Thing.asNode()))))

    val totalInstancesCount = totalInstances.count.toDouble
    val nodeList = graph.vertices.collect.toList

    var distanceRDD = spark.sparkContext.emptyRDD[(VertexId, VertexId, (Double, List[VertexId]))]
    var IC = spark.sparkContext.emptyRDD[(VertexId, Double)]
    

    //This part calculates the Information Content and the shortest distance from every node to every other node.
    for (w <- 0 until 10) {
      var sourceId: VertexId = VertexIds(w) // The ultimate source

      val shortestPathToAllNodes = findShortestPath(graph, sourceId)

      //var tempDistanceRDD = sssp2.vertices.map(v => (sourceId, v._1, v._2))
      var tempDistanceRDD = shortestPathToAllNodes.vertices.collect {
        case v if (wpathList.exists(p => (p._1 == sourceId) && (p._2 == v._1))) =>
          (sourceId, v._1, v._2)
      }

      distanceRDD = distanceRDD.union(tempDistanceRDD)

      //Finding IC part
      val nodeIC = calculateIC(triples, nodeList(w)._2, totalInstancesCount)

      var tempIC = spark.sparkContext.parallelize(List((nodeList(w)._1, nodeIC)))
      IC = IC.union(tempIC)

    }

    /*This part calculate the Least Common Subsumer for all the node pairs and the WPATH score.*/

    val depth = findDepth(distanceRDD, root)
    depth.foreach(println(_))   
    var depthCount = depth.length
    
    //var lcsList = spark.sparkContext.emptyRDD[(VertexId, VertexId, VertexId)] //The first two parameters are nodes and the last one is the LCS.
    var WPATH = spark.sparkContext.emptyRDD[(VertexId, VertexId, Double)]

    for (y <- 0 until 10) {

      if (!(wpathList(y)._1 == wpathList(y)._2)) {

        val node1 = wpathList(y)._1
        val node2 = wpathList(y)._2

        val newNodePath1 = depth.filter(f => (f._1 == node1)).flatMap(f => f._2._2)
        val newNodePath2 = depth.filter(f => (f._1 == node2)).flatMap(f => f._2._2)
        var lcs = findLCS(newNodePath1, newNodePath2, root)
        
        var tempDistance = distanceRDD.filter(f =>
          (f._1 == node1 && f._2 == node2))
          .map(f => f._3._1).first
        var ic = IC.filter(f => f._1 == lcs).map(f => f._2).first
        //wpath
        val wpath = calculateWpath(tempDistance, IC, lcs, k)      
        val tempWPATH = spark.sparkContext.parallelize(List((wpathList(y)._1, wpathList(y)._2, wpath)))
        WPATH = WPATH.union(tempWPATH)

      } 
      else {
        val tempWPATH = spark.sparkContext.parallelize(List((wpathList(y)._1, wpathList(y)._2, 1.0)))
        WPATH = WPATH.union(tempWPATH)
      }
    }

    WPATH.foreach(println(_))
    println(WPATH.count)
    

    spark.stop

  }

  def buildGraph(triples: RDD[Triple]): Graph[Node, Double] = {
    val rs = triples.map(triple => (triple.getSubject, triple.getPredicate, triple.getObject))
    val indexedMap = (rs.map(_._1) union rs.map(_._3)).distinct.zipWithUniqueId()

    val vertices: RDD[(VertexId, Node)] = indexedMap.map(x => (x._2, x._1))
    val _nodeToId: RDD[(Node, VertexId)] = indexedMap.map(x => (x._1, x._2))

    val tuples = rs.keyBy(_._1).join(indexedMap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[Double]] = tuples.join(indexedMap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, 1.0)
    })

    val reverseEdges: RDD[Edge[Double]] = edges.union(tuples.join(indexedMap).map({
      case (k, ((si, p), oi)) => Edge(oi, si, 1.0)
    }))

    val graph = Graph(vertices, reverseEdges)
    graph
  }

  def initialList(list: List[VertexId], listCount: Int): List[(VertexId, VertexId, Double)] = {
    var wpathList = List.empty[(VertexId, VertexId, Double)]

    //Create an initial list and prefilled the diagonal with similarity 1 and rest with -1
    for (w <- 0 until listCount) {
      for (y <- 0 until listCount) {
        if (w == y) {
          val tempWpathList = (list(w), list(y), 1.toDouble)
          wpathList = wpathList :+ tempWpathList
        } else {
          if (!wpathList.exists(p => (p._1 == list(y) && p._2 == list(w)))) {
            val tempWpathList = (list(w), list(y), -1.toDouble)
            wpathList = wpathList :+ tempWpathList
          }
        }
      }
    }
    wpathList
  }

  def findShortestPath(graph: Graph[Node, Double], sourceId: VertexId): Graph[(Double, List[VertexId]), Double] = {
    val initialGraph: Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) =>
      if (id == sourceId)
        (0.0, List[VertexId](sourceId))
      else
        (Double.PositiveInfinity, List[VertexId]()))

    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue)(

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

    sssp
  }

  //IC
  def calculateIC(triples: RDD[Triple], node: Node, totalInstancesCount: Double): Double = {
    var classInstances = triples
      .filter(triple => (triple.predicateMatches(RDF.`type`.asNode()) &&
        (triple.getSubject.isURI) &&
        (triple.objectMatches(node))))

    var classInstancesCount = classInstances.count.toDouble

    var ic = -(scala.math.log10(classInstancesCount / totalInstancesCount)).toDouble

    ic
  }

  //Find the depth of all nodes.
  def findDepth(distanceRDD: RDD[(VertexId, VertexId, (Double, List[VertexId]))], root: (VertexId, Node)): List[(VertexId, (Double, List[VertexId]))] = {
    val depth = distanceRDD.filter(f => f._1 == root._1).map(f => (f._2, f._3)).collect.toList

    depth
  }

  //Find LCS
  def findLCS(newNodePath1: List[VertexId], newNodePath2: List[VertexId], root: (VertexId, Node)): VertexId = {
    var j = 0
    var lcs: VertexId = root._1
    while (j < newNodePath1.length && j < newNodePath2.length) {
      if (newNodePath1(j) == newNodePath2(j)) {
        lcs = newNodePath1(j)
      }
      j = j + 1
    }
    
    lcs
  }
  
  //wpath
  def calculateWpath(tempDistance: Double, IC: RDD[(VertexId, Double)], lcs: VertexId, k: Double): Double = {
    var ic = IC.filter(f => f._1 == lcs).map(f => f._2).first   
    val wpath = 1.0 / (1.0 + (tempDistance * pow(k, ic)))
    
    wpath
  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("SANSA - Semantic Similarity example") {

    head(" SANSA - Semantic Similarity example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    help("help").text("prints this usage text")
  }

}
