package net.sansa_stack.template.spark.rdf

import scala.collection.mutable
import scala.math.pow

import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import net.sansa_stack.query.spark.query._
import org.apache.jena.graph.NodeFactory
import org.apache.jena.riot.Lang
import org.apache.jena.query.Query
import org.apache.spark.graphx
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{ Graph, VertexId, Edge }
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.rdf.spark.partition._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.vocabulary.{ OWL, RDF, RDFS, XSD }
import org.apache.spark.mllib.rdd.RDDFunctions._

object Semantic {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"Semantic Similarity  $input")
      //.master("spark://172.18.160.16:3077")
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

    //Get only the VertexIds of all the nodes.
    val VertexIds: Array[(VertexId, Node)] = graph.vertices.map(node => node).collect()
    val totalNodes = VertexIds.length

    //Total instances that we need every time we calculate IC of any node.
    val totalInstances = triples
      .filter(triple => (triple.predicateMatches(RDF.`type`.asNode()) &&
        (triple.getSubject.isURI) &&
        (triple.objectMatches(OWL.Thing.asNode()))))
    val totalInstancesCount = totalInstances.count
    
    var distanceRDD = spark.sparkContext.emptyRDD[(VertexId, VertexId, (Double, List[VertexId]))]
    //var SP = spark.sparkContext.emptyRDD[Graph[(Double, List[VertexId]), Double]]
    var IC = spark.sparkContext.emptyRDD[(Node, Double)]
    
    //Distance of all the nodes from the first node which is the root.
    val distanceFromRoot = findShortestPath(graph, root._1).vertices.map{ node => node}
    distanceFromRoot.foreach(println(_))
        
    var lcsList = spark.sparkContext.emptyRDD[(VertexId, VertexId, VertexId)] //The first two parameters are nodes and the last one is the LCS.
    var WPATH = spark.sparkContext.emptyRDD[(VertexId, VertexId, Double)]
    var ic = 0.0
    
    //Find LCS of all the nodes.
    for (i <- 0 until VertexIds.length - 1) {
      val newNodePath = distanceFromRoot
      .filter(vertex => vertex._1 == VertexIds(i)._1)
      .map(
        f => (VertexIds(i)._1, (f._2._2))    
      )

      
      for (j <- 1 until VertexIds.length) {
    		val newNodePath22 = distanceFromRoot
    		.filter(vertex => vertex._1 == VertexIds(j)._1)
    		.map(
          f => (VertexIds(i)._1, (f._2._2))    
        )

    		val test = newNodePath.join(newNodePath22)
    		
    		val result = test.map{
    		  case (key, (v1, v2)) => {
    			  var j = 0
    				var lcs: VertexId = root._1
    				while (j < v1.length && j < v2.length) {
    					if (v1(j) == v2(j)) {
    						lcs = v1(j)
    					}
    					j = j + 1
    				}
    			lcs
    		}
    		}
    	}
    }
    
    //This part calculates the Information Content and the shortest distance from every node to every other node.   
    for (i <- 0 until 14) {
                  
      val id1 = VertexIds(i)._1
      val node = VertexIds(i)._2
      val sssp = findShortestPath(graph, id1)

      val newNodePath = distanceFromRoot.filter(vertex => vertex._1 == id1).map(f => (i, (f._2._2)))
      val newNodePath1 = distanceFromRoot.filter(vertex => vertex._1 == id1).flatMap(f => f._2._2).take(1).toList

      for (j <- 1 until 15) {
        
                
        val id2 = VertexIds(j)._1
        val node2 = VertexIds(j)._2
        
        //If values are already calculated then don't do anything.
        if (WPATH.filter(id => ((id._1 == id1) && (id._2 == id2))).count > 0 ||
            WPATH.filter(id => ((id._2 == id1) && (id._1 == id2))).count > 0) {         
          
        }
        
        else {
        
        	//For the same node, the WPATH is 1.
        	if (id1 == id2) {
        		val tempWPATH = spark.sparkContext.parallelize(List((id1, id2, 1.0)))        
        		WPATH = WPATH.union(tempWPATH)
        	}

        	else {

        		val newNodePath22 = distanceFromRoot.filter(vertex => vertex._1 == id2).map(f => (i, (f._2._2)))
            val newNodePath2 = distanceFromRoot.filter(vertex => vertex._1 == id2).flatMap(f => f._2._2).take(1).toList
            
            val test = newNodePath.join(newNodePath22)
            
            val result = test.map{
        		  case (key, (v1, v2)) => {
        		    var j = 0
        		    var lcs: VertexId = root._1
        		    while (j < v1.length && j < v2.length) {
        		    	if (v1(j) == v2(j)) {
        		    		lcs = v1(j)
        		    	}
        		    	j = j + 1
        		    }

        		    lcs
        		  }
        		}
        		
        		//result.foreach(println("LCS is: ", _))
        		//println(id1, newNodePath1.length, id2, newNodePath2.length)
        		if (result.count > 0) {      		  
        		  //println("LCS of: ", id1, id2, "is: ", result.first)
        		}
            
        		val lcs = findLCS(newNodePath1, newNodePath2, root)
        		val lcsNode = VertexIds.filter(f => f._1 == lcs).map(f => f._2)
        		
        		//val tempLCS = spark.sparkContext.parallelize(List((id1, id2, result)))
        		//lcsList = lcsList.union(tempLCS)


        		if (lcsNode(0) == root._2) {
        		  ic = 0.0
        		}
        		else {
              ic = calculateIC(triples, lcsNode(0), totalInstancesCount)
        		}

        		val distanceTemp = sssp.vertices.filter(f => f._1 == id2).map(f => f._2._1).first

        		val wpath = calculateWpath(distanceTemp, ic, k)

        		if (distanceTemp == Double.PositiveInfinity) {

        			val tempWPATH = spark.sparkContext.parallelize(List((id1, id2, 0.0)))
        			WPATH = WPATH.union(tempWPATH)
        		}

        		else {
        			val tempWPATH = spark.sparkContext.parallelize(List((id1, id2, wpath)))
        			WPATH = WPATH.union(tempWPATH)
        		}
        		
        	}
        }
      }
    }
    
    val qPath = output + "/" + 7 + "/"
    //println(lcsList.count)
    //lcsList.foreach(println(_))
//    WPATH
//    .repartition(1)
//    .saveAsTextFile(qPath)    

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

    var classInstancesCount = classInstances.count

    var ic = -(scala.math.log10(classInstancesCount / totalInstancesCount)).toDouble

    ic
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
  def calculateWpath(tempDistance: Double, ic: Double, k: Double): Double = {
    val wpath = 1.0 / (1.0 + (tempDistance * pow(k, ic)))
    
    wpath
  }

  case class Config(in: String = "", out: String = "")

  val parser = new scopt.OptionParser[Config]("SANSA - Semantic Similarity example") {

    head(" SANSA - Semantic Similarity example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")
      
    opt[String]('o', "output").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }

}
