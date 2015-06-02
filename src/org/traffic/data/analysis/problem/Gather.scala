package org.traffic.data.analysis.problem

import java.text.SimpleDateFormat
import org.traffic.data.analysis.base.GPSRecord
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

class Gather(data:RDD[(String,Array[GPSRecord])]) extends Serializable{
  //将时间转化为和“2014-01-01 00：00：00的时间差，以秒为单位”
  private def StringToLong(time:String):Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    (formatter.parse(time).getTime - formatter.parse("2014-01-01 00:00:00").getTime) / 1000
  }
  //连续停车大于某个时间段的片段找出来
  private def getContinusParking(records:Array[GPSRecord],time:Long) = {
    var result = ArrayBuffer[(String)]() //连续停车时间大于time的数据
    //函数功能为：找到第一个连续停车序列，并返回处理的序列末尾，用来计算第二个连续序列
    def getListWithSpeedEqualsZero(records:List[GPSRecord]):List[GPSRecord] = {
      records match {
        case x::y => {
          if(x.speed == 0){
            //获取连续序列
            def fun(list:List[GPSRecord]):List[GPSRecord] = {
              list match {
                case lh::lt => {
                  if(lh.speed == 0) fun(lt)
                  else list
                }
                case _  => Nil
              }
            }
            val last = fun(y)
            if(!last.isEmpty) {
              if(StringToLong(last(0).time) - StringToLong(x.time) >= time)
                result += ((x.time + "," + last(0).time+ "," + x.latitude + "," + x.longitude))
              last
            } else Nil

          } else getListWithSpeedEqualsZero(y)
        }
        case _ => Nil
      }
    }
    //计算结果
    def writeResult(records:List[GPSRecord]):Int = {
      val list = getListWithSpeedEqualsZero(records)
      if(!list.isEmpty) writeResult(list)
      else return 0;
    }
    writeResult(records.toList);
    result.toArray
  }

  //聚集算法
  private def gather(cars:Array[String]) = {
    val cars_sorted = cars.sortBy(_.split(",")(1)).toList.reverse //按照开始从大到小排序
    val result = ArrayBuffer[Array[String]]()
    def getResult(cars: List[String]): Int = {
      cars match {
        case h :: t => {
          val t1 = StringToLong(h.split(",")(1)).toDouble / 3600
          val carsf = t.filter(line => StringToLong(line.split(",")(2)).toDouble / 3600 >= t1 + 1)
          if ((h :: carsf).length >= 5) result += (h :: carsf).toArray
          getResult(t)
        }
        case _ => 0
      }
    }
    getResult(cars_sorted)
    val finResult = new ArrayBuffer[Array[String]]()
    if(result.length >= 2){
      //结果去重
      finResult += result(0)
      for(i <- 1 until result.length){
        if(finResult.filter(_.containsSlice(result(i))).length == 0) finResult += result(i)
      }
      finResult
    }else result

  }

  def run = {
    data.flatMap{
      case (key,value) => {
        getContinusParking(value,3600).map((key+","+_))
      }
    }.map{
      case line => {
        val key = line.split(",")(3).substring(0,6) + "_" + line.split(",")(4).substring(0,7)
        (key,line)
      }
    }.groupByKey().filter(line => line._2.toArray.map(_.split(",")(0)).distinct.length >= 5)
    .map{
      case (key,value) => {
        val result = gather(value.toArray)
        (key,result)
      }
    }.filter(!_._2.isEmpty).flatMap{
      case (key,value) => {
        value.map(line => (key+"\n",line.reduce((a,b) => a + "\n" + b)))
      }
    }
  }
}
