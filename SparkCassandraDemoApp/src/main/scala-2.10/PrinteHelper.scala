object PrintHelper {

  def printIt(method : String, name : String, data : Any): Unit = {
    println(s"============ $method : $name $data")
  }

}
