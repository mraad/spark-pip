package com.esri

import com.vividsolutions.jts.geom._

/**
  */
object GeomFact extends Serializable {

  val geomFact = new GeometryFactory(new PrecisionModel(1000000.0))

  def createPoint(x: Double, y: Double) = {
    geomFact.createPoint(new Coordinate(x, y))
  }

  def createMultiPolygons(polygons: Array[Polygon]): Geometry = {
    geomFact.createMultiPolygon(polygons)
  }

  def createPolygon(shell: LinearRing, holes: Array[LinearRing]): Polygon = {
    geomFact.createPolygon(shell, holes)
  }
}
