package com.esri

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.geotools.geometry.jts.WKTReader2
import org.scalatest._

import scala.io.Source

/**
  */
class FeaturePolygonTest extends FlatSpec with Matchers {

  it should "read zero area geometry" in {
    val kryo = new Kryo()
    kryo.register(classOf[FeaturePolygon])

    val reader = new WKTReader2()
    Source
      .fromFile("/tmp/world.tsv")
      .getLines()
      .foreach(line => {
        val tokens = line.split("\t")
        val geom = reader.read(tokens(14))
        FeaturePolygon(geom, Array.empty[String])
          .toRowCols(4.0)
          .foreach {
            case (rowcol, feature) => {
              feature.geom.getGeometryType should endWith("Polygon")

              val baos = new ByteArrayOutputStream(4096)
              val output = new Output(baos)
              kryo.writeObject(output, feature)
              output.flush()

              val obj = kryo.readObject[FeaturePolygon](new Input(baos.toByteArray), classOf[FeaturePolygon])
              obj.geom.equalsExact(feature.geom, 0.000001)
            }
          }
      })
  }
}
