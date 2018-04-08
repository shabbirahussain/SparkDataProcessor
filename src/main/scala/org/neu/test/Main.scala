package org.neu.test

import java.util.{Date, Properties}

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.json.JSONObject

import scala.collection.JavaConverters._
import scala.util.Try
import java.io._

/**
  * @author shabbirahussain
  */
object Main extends Serializable {

  val prop: Properties = new Properties()
  try {
    val loader = this.getClass.getClassLoader
    val stream = loader.getResourceAsStream("config-defaults.properties")
    prop.load(stream)
    stream.close()
  } catch { case e: Exception => e.printStackTrace(); sys.exit(1)}

  val spark: SparkSession = SparkSession
    .builder()
    .master(prop.getProperty("spark.master"))
    .appName("ReactorLabs Git Miner")
    .config("spark.cores.max",       prop.getProperty("spark.cores.max"))
    .config("spark.executor.cores",  prop.getProperty("spark.executor.cores"))
    .config("spark.workers.cores",   prop.getProperty("spark.executor.cores"))
    .config("spark.executor.memory", prop.getProperty("spark.executor.memory"))
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")
  //////////////////////////////////////////////////////////////

  sc.setCheckpointDir("target/temp/spark/")
  val sqlContext: SQLContext = spark.sqlContext
  val logger:Logger = Logger.getLogger("project.default.logger")
  import spark.implicits._

  def main(args: Array[String])
  : Unit = {
    var start = 0l
    println("started at:" + new Date())
    start = System.currentTimeMillis()

    val source = args(0)
    val output = args(1)

    deleteRecursively(new File(output))

    val rawData = readFiles(source)
    val greoupSize = 6

    var rdd = getGroupedRdd(greoupSize, rawData).persist()
    var res = rdd
    for(i<- 1 until greoupSize){
      rdd = rdd.mapValues(rotateSeq).persist()
      res = res.union(rdd)
    }

    res.
      mapValues(x=> x.flatMap(y=> Seq(y._1, y._2, y._3))).
      map(x=> x._2.head ++ x._2.drop(2)).
      map(_.mkString(",")).
      coalesce(1, shuffle = false).
      saveAsTextFile(output)

    val pw = new PrintWriter(new File(output + "/part-00000.csv"))
    pw.println("Label," + (1 until greoupSize).
      flatMap(i=> Seq(("track_uri" + i), ("artist_uri" + i), ("album_uri" + i))).
      mkString(","))

    scala.io.Source.fromFile(output + "/part-00000").getLines().
      foreach(pw.println)
    pw.close()

    deleteRecursively(new File(output + "/part-00000"))
    println("\nended at:" + new Date() + "\ttook:"+ (System.currentTimeMillis() - start))
  }

  def rotateSeq(x: Iterable[(String, String, String)]) = {
    val lst = x.toSeq
    (lst.tail ++ Seq(x.head)).toIterable
  }

  def readFiles(path: String)
  : RDD[(Int, String, String, String)] = {
    sc.wholeTextFiles(path).
      map(x=> Try{
        val jObj = new org.json.JSONObject(x._2)
        jObj.getJSONArray("playlists").asScala.
          map(_.asInstanceOf[JSONObject]).
          flatMap(y=> {
            val pid = y.getInt("pid")
            y.getJSONArray("tracks").asScala.
              map(_.asInstanceOf[JSONObject]).
              map(z=> {
                val trackURI  = z.getString("track_uri")
                val artistURI = z.getString("artist_uri")
                val albumURI  = z.getString("album_uri")
                (pid, trackURI, artistURI, albumURI)
              })
          })
      }).filter(_.isSuccess).flatMap(_.get)
  }

  def getGroupedRdd(groupSize: Int, rdd: RDD[(Int, String, String, String)])
  : RDD[(Int, Iterable[(String, String, String)])] = {
    rdd.
      map(x=> (x._1, x._2.substring(14), x._3.substring(15), x._4.substring(14))).
//      map(x=> (x._1, x._2.hashCode, x._3.hashCode, x._4.hashCode)).
      map(x=> (x._1, (x._2, x._3, x._4))).
      groupByKey().
      flatMapValues(y => y.grouped(groupSize).toSeq).
      filter(_._2.size == groupSize)
  }

  def deleteRecursively(file: File)
  : Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete){
      //throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }
}