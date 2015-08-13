package com.esri

import java.io.{File, FileReader}
import java.util.Properties

import org.apache.spark.SparkConf

import scala.collection.JavaConverters._

/**
  */
object AppProperties {

  def loadProperties(filename: String, sparkConf: SparkConf) = {
    val file = new File(filename)
    if (file.exists()) {
      val reader = new FileReader(file)
      try {
        val properties = new Properties()
        properties.load(reader)
        properties.asScala.foreach { case (k, v) => sparkConf.set(k, v) }
      }
      finally {
        reader.close()
      }
    }
    sparkConf
  }
}