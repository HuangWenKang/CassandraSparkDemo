/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object SimpleApp {
  def main(args: Array[String]) {

    val conf = new SparkConf(true)
      .setAppName("Simple Application")
      .set("spark.cassandra.connection.host", "127.0.0.1")

    // Use local Spark (non clustered in this example) with 2 cores.
    // Note this relies on all the SBT dependencies being
    // downloaded to C:\Users\XXXXX\.ivy2 cache folder
    conf.setMaster("local[2]")
    implicit val sc = new SparkContext(conf)

    val tableReader = new CassandraTableReader()
    val tableWriter = new CassandraTableWriter()

    tableWriter.initialise()
    tableReader.readTestTableValues()
    tableWriter.writeTestTableValues()
    tableReader.readTestTableValues()
    tableReader.foreachTestTableValues()
    tableReader.getColumnAttributes()
    tableReader.getSets()
    tableReader.getUDT()
    tableReader.foreachSelectedTableColumnValues()
    tableReader.foreachFilteredTableColumnValues()
    tableReader.foreachTableRowAsTuples()
    tableReader.foreachTableRowAsCaseClasses()
    tableReader.foreachTableRowAsCaseClassesUsingColumnAliases()
    tableReader.foreachTableRowAsTuples()
    tableWriter.saveCollectionOfTuples()
    tableReader.foreachTableRowAsTuples()
    tableWriter.saveCollectionOfCaseClasses()
    tableWriter.saveUDT()

    println("====== DONE ======")

    readLine()
  }
}