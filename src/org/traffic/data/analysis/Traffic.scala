package org.traffic.data.analysis

import org.apache.spark.SparkContext
import org.traffic.data.analysis.base.BaseComputing
import org.traffic.data.analysis.problem.{OffLocation, Gather}
object Traffic {
  def main(args: Array[String]) {
    val sc  = new SparkContext()
    val data_gps = sc.textFile("/data/traffic/taxi/original_data/gps/GPS_2014_05_19")
    val data_trans = sc.textFile("/data/traffic/taxi/original_data/trans/TRANS_2014_05_19")
    val car_gps_info = BaseComputing.getCarGPSInfo(data_gps) //出租车每天的轨迹信息
   // val gatherResult = new Gather(car_gps_info).run //交通聚集
    val offLocationResult = new OffLocation(data_gps,data_trans).run
  }
}
