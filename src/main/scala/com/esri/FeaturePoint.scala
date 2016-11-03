package com.esri

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.vividsolutions.jts.geom.Geometry

/**
  */
case class FeaturePoint(var geom: Geometry, var attr: Array[String]) extends Feature with KryoSerializable {

  def this() = this(null, null)

  override def toRowCols(cellSize: Double): Seq[(RowCol, FeaturePoint)] = {
    val coordinate = geom.getCoordinate
    val c = (coordinate.x / cellSize).floor.toInt
    val r = (coordinate.y / cellSize).floor.toInt
    Seq((RowCol(r, c), this))
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    val coordinate = geom.getCoordinate
    output.writeDouble(coordinate.x)
    output.writeDouble(coordinate.y)
    output.writeInt(attr.length)
    attr.foreach(output.writeString)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    val x = input.readDouble()
    val y = input.readDouble()
    geom = GeomFact.createPoint(x, y)
    val len = input.readInt()
    attr = Array.ofDim[String](len)
    for (i <- 0 until len)
      attr(i) = input.readString()
  }
}
