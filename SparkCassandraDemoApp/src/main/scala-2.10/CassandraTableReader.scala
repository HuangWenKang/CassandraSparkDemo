import org.apache.spark.SparkContext
import com.datastax.spark.connector._

class CassandraTableReader()(implicit val sc : SparkContext) {

  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def readTestTableValues(): Unit = {
    println(s"In method : readTestTableValues")
    val rdd = sc.cassandraTable("test", "kv")
    val count = rdd.count
    val first = rdd.first()
    val sum = rdd.map(_.getInt("value")).sum
    println(s"============ RDD COUNT $count")
    println(s"============ RDD FIRST $first")
    println(s"============ RDD SUM $sum")
  }


  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def foreachTestTableValues(): Unit = {
    println(s"In method : foreachTestTableValues")
    val rdd = sc.cassandraTable("test", "kv")
    rdd.foreach(println)
  }


  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def getColumnAttributes(): Unit = {
    println(s"In method : getColumnValue")
    val rdd = sc.cassandraTable("test", "kv")
    val firstRow = rdd.first
    val colNames = firstRow.columnNames
    val colSize = firstRow.size
    val firstKey = firstRow.get[String]("key")
    val firstValue = firstRow.get[Int]("value")
    println(s"============ RDD COLUMN NAMES $colNames")
    println(s"============ RDD COLUMN SIZE $colSize")
    println(s"============ RDD FIRST KEY $firstKey")
    println(s"============ RDD FISRT $firstValue")

  }

  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def getSets(): Unit = {
    println(s"In method : getSets")
    val rdd = sc.cassandraTable("test", "users")
    val row = rdd.first()
    PrintHelper.printIt("getSets", "List[String]", row.get[List[String]]("emails"))
    PrintHelper.printIt("getSets", "IndexedSeq[String]", row.get[IndexedSeq[String]]("emails"))
    PrintHelper.printIt("getSets", "Seq[String]", row.get[Seq[String]]("emails"))
    PrintHelper.printIt("getSets", "Set[String]", row.get[Set[String]]("emails"))
  }


  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
  def getUDT(): Unit = {
    println(s"In method : getUDT")
    val rdd = sc.cassandraTable("test", "companies")
    val row = rdd.first()
    val address: UDTValue = row.getUDTValue("address")
    PrintHelper.printIt("getUDT", "city", address.getString("city"))
    PrintHelper.printIt("getUDT", "street", address.getString("street"))
    PrintHelper.printIt("getUDT", "number", address.getString("number"))
  }

  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/3_selection.md
  def foreachSelectedTableColumnValues(): Unit = {
    println(s"In method : foreachSelectedTableColumnValues")

    val filteredColumnsRdd = sc.cassandraTable("test", "users")
      .select("username")

    filteredColumnsRdd.foreach(println)

    val row = filteredColumnsRdd.first()
    PrintHelper.printIt("foreachSelectedTableColumnValues", "username", row.getString("username"))
  }


  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/3_selection.md
  def foreachFilteredTableColumnValues(): Unit = {
    println(s"In method : foreachFilteredTableColumnValues")

    val filteredColumnsRdd = sc.cassandraTable("test", "users")
      .select("username")
      .where("username = ?", "bill")

    filteredColumnsRdd.foreach(println)

    val row = filteredColumnsRdd.first()
    PrintHelper.printIt("foreachFilteredTableColumnValues", "username", row.getString("username"))
  }


  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/3_selection.md
  def foreachTableRowCount(): Unit = {
    println(s"In method : foreachTableRowCount")

    val count = sc.cassandraTable("test", "users")
      .select("username")
      .cassandraCount()

    PrintHelper.printIt("foreachTableRowCount", "all users count", count)
  }

  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/4_mapper.md
  def foreachTableRowAsTuples(): Unit = {
    println(s"In method : foreachTableRowAsTuples")

    val rdd = sc.cassandraTable[(String, Int)]("test", "words")
      .select("word", "count");
    val items = rdd.take(rdd.count().asInstanceOf[Int])
    items.foreach(tuple => PrintHelper.printIt("foreachTableRowAsTuples",
      "tuple(String, Int)", tuple))

    val rdd2 = sc.cassandraTable[(Int, String)]("test", "words")
      .select("count", "word")
    val items2 = rdd2.take(rdd2.count().asInstanceOf[Int])
    items2.foreach(tuple => PrintHelper.printIt("foreachTableRowAsTuples",
      "tuple(Int, String)", tuple))
  }

  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/4_mapper.md
  def foreachTableRowAsCaseClasses(): Unit = {
    println(s"In method : foreachTableRowAsCaseClasses")

    val items = sc.cassandraTable[WordCount]("test", "words")
      .select("word", "count").take(3)

    items.foreach(wc => PrintHelper.printIt("foreachTableRowAsCaseClasses",
      "WordCount(word : String, count : Int)", wc))
  }


  //See this post for more details
  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/4_mapper.md
  def foreachTableRowAsCaseClassesUsingColumnAliases(): Unit = {
    println(s"In method : foreachTableRowAsCaseClassesUsingColumnAliases")

    val items = sc.cassandraTable[WordCount]("test", "kv")
      .select("key" as "word", "value" as "count").take(1)

    items.foreach(wc => PrintHelper.printIt("foreachTableRowAsCaseClassesUsingColumnAliases",
      "WordCount(word : String, count : Int)", wc))
  }


}
