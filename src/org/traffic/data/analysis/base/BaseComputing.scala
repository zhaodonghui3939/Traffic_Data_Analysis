package org.traffic.data.analysis.base

import org.apache.spark.rdd.RDD
object BaseComputing {
  //获取车辆信息gps集合，转化为GPSRecord存储
  def getCarGPSInfo(data:RDD[String]):RDD[(String,Array[GPSRecord])] = {
    data.map{
      case record => {
        val carId = record.split(",")(0)
        (carId,record)
      }
    }.groupByKey().map{
      case (key,value) => {
        (key,value.toArray.distinct.map(new GPSRecord(_)).sortBy(_.time))   //数据去重并转化为GPSRecord对象,按照时间排序
      }
    }
  }
}
