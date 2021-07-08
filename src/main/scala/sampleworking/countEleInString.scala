package sampleworking

import scala.{+:, ::}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object countEleInString {
  def main(args: Array[String]): Unit = {
    var inStr: String = "aaabbcfa"
    //var mData : mutable.HashMap[Char, Int] = mutable.HashMap[Char,Int]()
    var mData :ListBuffer[String] = ListBuffer[String]()
    var count = 1
    for(i <- 0 until inStr.length){
      if((i < inStr.length-1) && inStr.charAt(i).equals(inStr.charAt(i+1)) ){
        count = count + 1
      } else {
        println(s"ele is ${inStr.charAt(i)} , ${count}")
        mData += {inStr.charAt(i)}.toString
        mData += {count}.toString
        count = 1
      }
    }
    println("res is " + mData)









  }

}
