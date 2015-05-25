package org.traffic.data.analysis.base
//GPS轨迹信息数据结构
class GPSRecord(val record:String) extends Serializable{
  private val ss = record.split(",")
  val carId = ss(0)      //车牌id
  val longitude = ss(1)  //经度
  val latitude = ss(2)   //纬度
  val time = ss(3)       //时间
  val speed = ss(5).toInt //速度
  override def toString = {
    record
  }
}
