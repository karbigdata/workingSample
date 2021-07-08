package sampleworking

object FindVowels {
  def main(args: Array[String]): Unit = {
    val myStr = "Welcome to Hyderabad"
    val vowelList = "aeiou"
    //contains return boolean so counting the true boolean
    val res2 = myStr.toLowerCase.count(ele => vowelList.contains(ele))
    println("res is " + res2 )
  }
}
