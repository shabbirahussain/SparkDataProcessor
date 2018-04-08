package org.neu.test

import java.util.{Date, Properties}

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SparkSession}
import org.json.JSONObject

import scala.collection.JavaConverters._
import scala.util.Try
import java.io._

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

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
  } catch {
    case e: Exception => e.printStackTrace(); sys.exit(1)
  }

  val spark: SparkSession = SparkSession
    .builder()
    .master(prop.getProperty("spark.master"))
    .appName("ReactorLabs Git Miner")
    .config("spark.cores.max", prop.getProperty("spark.cores.max"))
    .config("spark.executor.cores", prop.getProperty("spark.executor.cores"))
    .config("spark.workers.cores", prop.getProperty("spark.executor.cores"))
    .config("spark.executor.memory", prop.getProperty("spark.executor.memory"))
    .config("spark.network.timeout", prop.getProperty("spark.network.timeout"))
    .config("spark.executor.heartbeatInterval", prop.getProperty("spark.executor.heartbeatInterval"))
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")
  //////////////////////////////////////////////////////////////

  sc.setCheckpointDir("target/temp/spark/")
  val sqlContext: SQLContext = spark.sqlContext
  val logger: Logger = Logger.getLogger("project.default.logger")

  import spark.implicits._

  def main(args: Array[String])
  : Unit = {
    var start = 0l
    println("started at:" + new Date())
    start = System.currentTimeMillis()

    val source1 = args(0)
    val source2 = args(1)
    val output  = args(2)

    deleteRecursively(new File(output))

    val playlists = readPlaylists(source1).
      withColumn("pname", regexp_replace(lower($"pname"), "[^a-z]", ""))

    val songInfo  = readSongInfo(source2).
      select(
        regexp_replace(lower($"title"), "[^a-z]", "").as("title"),
        regexp_replace(lower($"artist_name"), "[^a-z]", "").as("artist_name"),
        regexp_replace($"song_hotttnesss", lit("NA"), lit(0)).as("song_hotttnesss"),
        when($"year" === 0, lit(null)).as("year"),
        $"loudness", $"tempo", $"key",$"mode", $"time_signature", $"danceability",$"energy",$"artist_familiarity",
        $"artist_hotttnesss"
      )

    val joined = playlists.
      join(songInfo, usingColumns = Seq("title", "artist_name")).
      persist(StorageLevel.DISK_ONLY)

    val aggCols  = Seq("loudness", "tempo", "key", "mode", "time_signature", "artist_familiarity", "artist_hotttnesss", "song_hotttnesss", "year")
    val headCols = Seq("title", "artist_name", "pname")
    val cols     =  headCols ++ aggCols

    val mapping: Map[String, Column => Column] = Map("sum" -> sum, "count" -> count)
    val exprs = aggCols.
      flatMap(c=> Seq("sum", "count").map(f=> mapping(f)(col(c)).over(Window.partitionBy("pname")).as("%s(%s)".format(f,c)))) ++
      (aggCols ++ headCols).map(col)

    val test = joined.
      select(exprs:_*).
      filter($"count(loudness)" > 1).
      select(
        headCols.map(col) ++
          aggCols.map(x=> ((col("sum(%s)".format(x)) - col(x))/(col("count(%s)".format(x)) - 1.0)).as(x)):_*
      )

    joined.
      select(cols.map(col):_*).
      rdd.map(_.mkString(",")).
      coalesce(1, shuffle = false).
      saveAsTextFile(output + "/train/")
    addHeadersToFile(output + "/train/", cols)

    test.select(cols.map(col):_*).
      rdd.map(_.mkString(",")).
      coalesce(1, shuffle = false).
      saveAsTextFile(output + "/test/")
    addHeadersToFile(output + "/test/", cols)

    println("\nended at:" + new Date() + "\ttook:" + (System.currentTimeMillis() - start))
  }

  def addHeadersToFile(output: String, cols: Seq[String]): Unit = {
    val pw = new PrintWriter(new File(output + "/part-00000.csv"))
    pw.println(cols.mkString(","))
    scala.io.Source.fromFile(output + "/part-00000").getLines().foreach(pw.println)
    pw.close()
    deleteRecursively(new File(output + "/part-00000"))
  }


  def readPlaylists(path: String)
  : DataFrame = {
    sc.wholeTextFiles(path).
      map(x => Try {
        val jObj = new org.json.JSONObject(x._2)
        jObj.getJSONArray("playlists").asScala.
          map(_.asInstanceOf[JSONObject]).
          flatMap(y => {
            val pid = y.getString("name")
            y.getJSONArray("tracks").asScala.
              map(_.asInstanceOf[JSONObject]).
              map(z => {
                val track  = z.getString("track_name") .toLowerCase.replaceAll("[^a-z]","")
                val artist = z.getString("artist_name").toLowerCase.replaceAll("[^a-z]","")
//                val album  = z.getString("album_name") .toLowerCase.replaceAll("[^a-z]","")
                (pid, track, artist)
              })
          })
      }).filter(_.isSuccess).flatMap(_.get).
      toDF("pname", "title", "artist_name")
  }

  def readSongInfo(path: String)
  : DataFrame = {
    //track_id;audio_md5;end_of_fade_in;start_of_fade_out;analysis_sample_rate;duration;loudness;tempo;key;key_confidence;mode;mode_confidence;time_signature;time_signature_confidence;danceability;energy;artist_id;artist_name;artist_location;artist_familiarity;artist_hotttnesss;genre;release;song_id;title;song_hotttnesss;year
    sqlContext.read.format("com.databricks.spark.csv").
      option("header", "true").
      option("inferSchema", "true").
      option("delimiter", ";").
      option("charset", "UTF-8").
      option("mode", "DROPMALFORMED").
      load(path)
  }

  def deleteRecursively(file: File)
  : Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete) {
      //throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }
}