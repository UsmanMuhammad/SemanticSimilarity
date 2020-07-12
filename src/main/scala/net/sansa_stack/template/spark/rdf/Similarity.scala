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

class Similarity(
  numOfFilesPartition: Int,
  triples:             RDD[Triple],
  input:               String,
  output:              String,
  k:                   Double) extends Serializable {

  /*WPATH, the final RDD is in the form of
     * RDD[((Vertex1, Vertex2), (LCS of both, IC of LCS), WPATH value)]
     */
  var WPATH: RDD[((VertexId, VertexId), (VertexId, Double), Double)] = _

  def run(): Unit = {

    //convert the RDD into a graph
    val graph = this.buildGraph(triples)

    val root = graph.vertices.first

    val zero = 0
    var countChek: Int = 0

    var ic = 0.0

    //Get only the VertexIds of all the nodes.
    val VertexIdsRDD = graph.vertices.map(node => node)
    val VertexIds: Array[(VertexId, Node)] = graph.vertices.map(node => node).collect()
    val totalNodes = VertexIds.length

    //Total instances that we need every time we calculate IC of any node.
    val totalInstances = triples
      .filter(triple => (triple.predicateMatches(RDF.`type`.asNode()) &&
        (triple.getSubject.isURI) &&
        (triple.objectMatches(OWL.Thing.asNode()))))
    val totalInstancesCount = totalInstances.count

    //Distance of all the nodes from the first node which is the root.
    val distanceFromRoot = this.findShortestPath(graph, root._1).vertices.map { node => node }

    //This part calculates the Information Content and the shortest distance from every node to every other node.
    for (i <- 0 until totalNodes - 1) {

      val id1 = VertexIds(i)._1
      val node = VertexIds(i)._2
      
      val totalNodes1 = totalNodes

      val sssp = this.findShortestPath(graph, id1)

      val newNodePath1 = distanceFromRoot
        .filter(vertex => vertex._1 == id1)
        .map(
          f => (id1, (f._2._2)))
          

      for (j <- 1 until totalNodes1 - 1) {
        countChek += 1

        val id2 = VertexIds(j)._1
        val node2 = VertexIds(j)._2

        val newNodePath2 = distanceFromRoot
          .filter(vertex => vertex._1 == id2)
          .map(
            f => (id1, (f._2._2)))


        val JoinedNodePaths = newNodePath1.join(newNodePath2)

        val lcs = this.findLCS(JoinedNodePaths, root._1)

        val lcsNode = graph.vertices.filter(f => f._1 == lcs).map(f => f._2).take(1)(0)

        if (lcs == root._1) {
          ic = 0.0
        } else {
          ic = this.calculateIC(triples, lcsNode, totalInstancesCount)
        }

        //val tempIC = spark.sparkContext.parallelize(List((lcs, lcsNode, ic)))
        //IC = IC.union(tempIC)

        val distanceTemp = sssp.vertices.filter(f => f._1 == id2).map(f => f._2._1)

        val wpath = this.calculateWpath(distanceTemp.take(1)(0), ic, k)

        var tempWPATH = distanceTemp
          .map(
            node => {
              //if both are same, then wpath is 1
              if (id1 == id2) {
                ((id1, id2), (id1, 1.0), 1.0)
              } else {

                if (distanceTemp == Double.PositiveInfinity) {
                  ((id1, id2), (lcs, ic), 0.0)
                } else {
                  ((id1, id2), (lcs, ic), wpath)
                }
              }
            })
        if (countChek == 1) {
          WPATH = tempWPATH
        } else {
          if (WPATH.filter(
            id => ((id._1 == id1) && (id._2 == id2)) ||
              ((id._1 == id2) && (id._2 == id1)))
            .take(1)
            .length > 0) {

          } else {
            WPATH = WPATH.union(tempWPATH)
          }
        }
      }
    }

    var path = output + "/results/"

    WPATH
      .repartition(1)
      .saveAsTextFile(path)
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
  def findLCS(JoinedNodePaths: RDD[(VertexId, (List[VertexId], List[VertexId]))], root: VertexId): VertexId = {
    var j = 0
    val lcs = JoinedNodePaths.map {
      case (key, (v1, v2)) => {
        var lcs: VertexId = root

        while (j < v1.length && j < v2.length) {
          if (v1(j) == v2(j)) {
            lcs = v1(j)
          }
          j = j + 1
        }

        lcs
      }
    }.take(1)

    lcs(0)
  }

  //wpath
  def calculateWpath(tempDistance: Double, ic: Double, k: Double): Double = {
    val wpath = 1.0 / (1.0 + (tempDistance * pow(k, ic)))

    wpath
  }
}

