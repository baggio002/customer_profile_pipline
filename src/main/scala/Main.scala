import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object Main {
    def main(args: Array[String]): Unit =
        println("Hello world!")
        connectBq();

    def msg = "I was compiled by Scala 3. :)"

    def connectBq(): Unit = {
        val spark = SparkSession.builder
           .appName(this.getClass.getName)
           .master("local[*]")
           .getOrCreate() 
        println("test bq connection===");
        val df = spark.read
           .format("bigquery")
           .load("zhaohu-test3.case_detail.t_cases_details_source")
        // val df = spark.sql("SELECT * FROM `zhaohu-test3.test1.test1`");
        df.show();
    }

}
