package org.traffic.data.analysis.problem

import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import org.traffic.data.analysis.base.{ GPSRecord}
//计算下车地点
class OffLocation(gps:RDD[String],trans:RDD[String]) extends Serializable{
  //将时间转化为和“2014-01-01 00：00：00的时间差，以秒为单位”
  private def StringToLong(time:String):Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    (formatter.parse(time).getTime - formatter.parse("2014-01-01 00:00:00").getTime) / 1000
  }
  private def findearest(gpsInfo:Array[Long],endTime:Long):Int = {//gpsInfo已经从小到大排序好了
    var low = 0
    var high =  gpsInfo.length - 1
    var mid = 0
    while(low <= high){  //二分查找寻找离时间最近的gps轨迹
      mid = (low + high) / 2
      if (endTime == (gpsInfo(mid))) return mid
      else if(endTime < gpsInfo(mid)) high = mid -1
      else low = mid + 1
    }
    return mid
  }

  private val gpsInfo = gps.map{
    case record => {
      val ss = record.split(",")
      val carId = ss(0);
      (carId,record)
    }
  }.groupByKey().map{
    case (carId,values) => {
      (carId,values.toArray.distinct.sortBy(_.split(",")(3)))
    }
  }

  private val transInfo = trans.map{
    case record => {
      val ss = record.split(",")
      val carId = ss(0)
      (carId,record)
    }
  }.groupByKey().map{
    case (carId,values) => {
      (carId,values.toArray.distinct)
    }
  }

  def run = {
    transInfo.join(gpsInfo).flatMap{
      case (carId,(trans,gps)) => {
        val gpsTime = gps.map(_.split(",")(3)).map(StringToLong(_))
        trans.map(record => {
          val index = findearest(gpsTime,StringToLong(record.split(",")(2)))
          val ss = gps(index).split(",")
          (ss(1)+"_"+ss(2),carId)
        })
      }
    }
  }
}
