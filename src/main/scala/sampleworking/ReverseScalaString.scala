package sampleworking
import scala.Array
object ReverseScalaString {
  def main(args: Array[String]): Unit = {
    val myStr = "I am working on bigdata with scala"
    val myRes = myStr.split(" ").foldRight(Array[String]())((a,b) => b :+a ).mkString(" ")
    println("res us " + myRes)
    }
  }

