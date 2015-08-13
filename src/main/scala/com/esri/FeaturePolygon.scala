package com.esri

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.vividsolutions.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import com.vividsolutions.jts.geom.{Coordinate, Geometry, LinearRing, Polygon}

/**
  */
case class FeaturePolygon(var geom: Geometry, var attr: Array[String]) extends Feature with KryoSerializable {
  override def toRowCols(cellSize: Double): Seq[(RowCol, Feature)] = {
    val envelope = geom.getEnvelopeInternal
    val cmin = (envelope.getMinX / cellSize).floor.toInt
    val cmax = (envelope.getMaxX / cellSize).floor.toInt
    val rmin = (envelope.getMinY / cellSize).floor.toInt
    val rmax = (envelope.getMaxY / cellSize).floor.toInt
    for (r <- rmin to rmax; c <- cmin to cmax)
      yield (RowCol(r, c), this)
  }

  @transient
  var preparedGeometry: PreparedGeometry = _

  def prepare(preparedGeometryFactory: PreparedGeometryFactory) = {
    preparedGeometry = preparedGeometryFactory.create(geom)
  }

  def contains(other: Geometry) = {
    preparedGeometry.contains(other)
  }

  private def writeCoords(coordinates: Array[Coordinate], output: Output): Unit = {
    output.writeInt(coordinates.length)
    coordinates.foreach(c => {
      output.writeDouble(c.x)
      output.writeDouble(c.y)
    })
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    val numGeometries = geom.getNumGeometries
    output.writeInt(numGeometries)
    for (n <- 0 until numGeometries) {
      val polygon = geom.getGeometryN(n).asInstanceOf[Polygon]
      writeCoords(polygon.getExteriorRing.getCoordinates, output)
      val numInteriorRing = polygon.getNumInteriorRing
      output.writeInt(numInteriorRing)
      for (i <- 0 until numInteriorRing) {
        writeCoords(polygon.getInteriorRingN(i).getCoordinates, output)
      }
    }
    output.writeInt(attr.length)
    attr.foreach(output.writeString)
  }

  private def readLinearRing(input: Input) = {
    val numCoords = input.readInt()
    val coords = Array.ofDim[Coordinate](numCoords)
    for (n <- 0 until numCoords) {
      val x = input.readDouble()
      val y = input.readDouble()
      coords(n) = new Coordinate(x, y)
    }
    GeomFact.geomFact.createLinearRing(coords)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    val numGeometries = input.readInt()
    val polygons = Array.ofDim[Polygon](numGeometries)
    for (n <- 0 until numGeometries) {
      val shell = readLinearRing(input)
      val numHoles = input.readInt()
      val holes = Array.ofDim[LinearRing](numHoles)
      for (h <- 0 until numHoles) {
        holes(h) = readLinearRing(input)
      }
      polygons(n) = GeomFact.createPolygon(shell, holes)
    }
    geom = GeomFact.createMultiPolygons(polygons)
    val len = input.readInt()
    attr = Array.ofDim[String](len)
    for (i <- 0 until len)
      attr(i) = input.readString()
  }
}
