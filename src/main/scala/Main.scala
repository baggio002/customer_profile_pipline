import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object Main {

    val spark = SparkSession.builder
           .appName(this.getClass.getName)
           .master("local[*]")
           .getOrCreate()    

    def readData(): DataFrame = {
        spark.read
           .format("bigquery")
           .load("zhaohu-test3.case_detail.t_cases_details_source")
    }
    
    def main(args: Array[String]): Unit = {
        println("Hello world!");
        // connectBq();
        // connectPostgresql();
        processData(readData())
    }

    def processData(df: DataFrame): Unit = {
        // df.show();
        df.groupBy("customer_id", "customer_name", "industry", "customer_is_hypercare")
            .count()
            .withColumnRenamed("count", "case_count")
            .select("customer_id","customer_name", "industry", "customer_is_hypercare", "case_count")
            .show()
    }

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

    def connectPostgresql(): Unit = {
        // psql -h '10.105.64.4' -d 'postgres' -U 'postgres'
        val spark = SparkSession.builder
           .appName(this.getClass.getName)
           .master("local[*]")
           .getOrCreate();
        val jdbcUrl = "jdbc:postgresql://10.105.64.4:5432/customer-profile"
        val connectionProperties = new java.util.Properties()
        connectionProperties.setProperty("user", "postgres")
        connectionProperties.setProperty("password", "!Zidane820917")
        connectionProperties.setProperty("driver", "org.postgresql.Driver")

        val df = spark.read.format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", "customer_profile") // Or .option("query", "SELECT * FROM mytable WHERE condition")
            .option("user", "postgres")
            .option("password", "!Zidane820917")
            .option("driver", "org.postgresql.Driver")
            // .options(connectionProperties)
            .load()

        df.show()

    }

}
