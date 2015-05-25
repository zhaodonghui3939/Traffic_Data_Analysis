package org.traffic.data.analysis

import org.apache.spark.SparkContext
import org.traffic.data.analysis.base.BaseComputing
import org.traffic.data.analysis.problem.Gather
object Traffic {
  def main(args: Array[String]) {
    val sc  = new SparkContext()
    val data = sc.textFile("/data/traffic/taxi/original_data/gps/GPS_2014_05_20")
    val carInfo = BaseComputing.getCarInfo(data).cache()
    val tem = new Gather(carInfo).run
  }
}
