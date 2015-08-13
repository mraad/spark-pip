package com.esri

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable

/**
  */
object AppMain extends App with Logging {

  val sparkConf = new SparkConf()
    .setAppName("Spark PiP")
    .registerKryoClasses(Array(
      classOf[Feature],
      classOf[FeaturePoint],
      classOf[FeaturePolygon],
      classOf[Point],
      classOf[Polygon],
      classOf[RowCol]
    ))

  val propFileName = if (args.length == 0) "application.properties" else args(0)
  AppProperties.loadProperties(propFileName, sparkConf)

  val t1 = System.currentTimeMillis()
  val sc = new SparkContext(sparkConf)
  try {
    val conf = sc.getConf

    val geomFact = new GeometryFactory(new PrecisionModel(conf.getDouble("geometry.precision", 1000000.0)))

    val minLon = conf.getDouble("extent.xmin", -180.0)
    val maxLon = conf.getDouble("extent.xmax", 180.0)
    val minLat = conf.getDouble("extent.ymin", -90.0)
    val maxLat = conf.getDouble("extent.ymax", 90.0)

    def pointInPolygon(t: (RowCol, Iterable[Feature])) = {

      val iter = t._2

      val points = mutable.Buffer[FeaturePoint]()
      val polygons = mutable.Buffer[FeaturePolygon]()

      val preparedGeometryFactory = new PreparedGeometryFactory()
      iter.foreach(elem => {
        elem match {
          case point: FeaturePoint => points += point
          case polygon: FeaturePolygon => {
            polygon.prepare(preparedGeometryFactory)
            polygons += polygon
          }
        }
      })

      points.flatMap(point => {
        for {
          polygon <- polygons
          if polygon.contains(point.geom)
        } yield {
          point.attr ++ polygon.attr
        }
      })
    }

    val envp = new Envelope(minLon, maxLon, minLat, maxLat)

    val pointSep = conf.get("points.sep", "\t").charAt(0)
    val pointLon = conf.getInt("points.x", 0)
    val pointLat = conf.getInt("points.y", 1)
    val pointIdx = conf.get("points.fields", "").split(',').map(_.toInt)

    val pointRDD: RDD[Feature] = sc.textFile(conf.get("points.path"))
      .flatMap(line => {
        try {
          val splits = line.split(pointSep)
          val lon = splits(pointLon).toDouble
          val lat = splits(pointLat).toDouble
          val geom = geomFact.createPoint(new Coordinate(lon, lat))
          if (geom.getEnvelopeInternal.intersects(envp)) {
            Some(FeaturePoint(geom, pointIdx.map(splits(_))))
          }
          else
            None
        }
        catch {
          case _: Throwable => None
        }
      })

    lazy val reader = new WKTReader(geomFact)

    val polygonSep = conf.get("polygons.sep", "\t").charAt(0)
    val polygonWKT = conf.getInt("polygons.wkt", 0)
    val polygonIdx = conf.get("polygons.fields", "").split(',').map(_.toInt)

    val polygonRDD: RDD[Feature] = sc.textFile(conf.get("polygons.path"))
      .flatMap(line => {
        try {
          val splits = line.split(polygonSep)
          val geom = reader.read(splits(polygonWKT))
          if (geom.getEnvelopeInternal.intersects(envp))
            Some(FeaturePolygon(geom, polygonIdx.map(splits(_))))
          else
            None
        }
        catch {
          case _: Throwable => None
        }
      })

    val reduceSize = conf.getDouble("reduce.size", 1.0)

    val outputSep = conf.get("output.sep", "\t")

    pointRDD.
      union(polygonRDD).
      flatMap(_.toRowCols(reduceSize)).
      groupByKey().
      flatMap(pointInPolygon).
      map(_.mkString(outputSep)).
      saveAsTextFile(conf.get("output.path"))

  } finally {
    sc.stop()
  }
  val t2 = System.currentTimeMillis()

  log.info("Processing time %d sec".format((t2 - t1) / 1000))

}