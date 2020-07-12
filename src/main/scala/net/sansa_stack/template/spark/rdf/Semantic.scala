package net.sansa_stack.template.spark.rdf

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.NodeFactory
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.io.IOException

object Semantic {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out, config.constant)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String, k: Double): Unit = {

    removePathFiles(Paths.get(output))
    
    val spark = SparkSession.builder
      .appName("Semantic Similarity")
      .master("spark://172.18.160.16:3090")
      //.master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println(" | Similarity Measure - Wpath method |")
    println("======================================")

    val numOfFilesPartition: Int = 1
    val lang = Lang.NTRIPLES

    //Data in triples
    val triples = spark.rdf(lang)(input)
    
    
    val similarity = new Similarity (
        numOfFilesPartition,
        triples,
        input,
        output,
        k
    )
    
    similarity.run()
        

    spark.stop

  }

  
  // remove path files
    def removePathFiles(root: Path): Unit = {
        if (Files.exists(root)) {
            Files.walkFileTree(root, new SimpleFileVisitor[Path] {
                override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                    Files.delete(file)
                    FileVisitResult.CONTINUE
                }

                override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
                    Files.delete(dir)
                    FileVisitResult.CONTINUE
                }
            })
        }
    }

  case class Config(in: String = "", out: String = "", constant: Double = 0.0)

  val parser = new scopt.OptionParser[Config]("SANSA - Semantic Similarity example") {

    head(" SANSA - Semantic Similarity example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")
      
    opt[String]('o', "output").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")
      
    //Jaccard similarity threshold value
    opt[Double]('k', "constant").required().
      action((x, c) => c.copy(constant = x)).
      text("k constant for wpath")

    help("help").text("prints this usage text")
  }

}
