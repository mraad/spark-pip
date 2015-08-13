package com.esri

/**
  */
trait Feature extends Serializable {
  def toRowCols(cellSize: Double): Seq[(RowCol, Feature)]
}
