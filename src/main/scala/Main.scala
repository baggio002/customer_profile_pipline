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
            .load("bigquery-public-data.austin_311.311_service_requests")
        df.show();
    }

}
