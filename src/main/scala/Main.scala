import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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
        /**
        df.groupBy("customer_id", "customer_name", "industry", "customer_is_hypercare")
            .count()
            .withColumnRenamed("count", "case_count")
            .select("customer_id","customer_name", "industry", "customer_is_hypercare", "case_count", "data_case_count")
            .show()**/
        df.groupBy("customer_id", "customer_name", "industry", "customer_is_hypercare")
          .agg(
               count("*").as("total_case_count"),
               sum(when(col("shard")==="Data", 1).otherwise(0)).as("data_case_count"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")<=3, 1).otherwise(0)).as("data_3d_close"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("has_escalation"), 1).otherwise(0)).as("data_3d_escalation"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("feedback_is_effortless"), 1).otherwise(0)).as("data_3d_feedback_is_effortless"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("data_3d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7, 1).otherwise(0)).as("data_7d_close"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("has_escalation"), 1).otherwise(0)).as("data_7d_escalation"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("feedback_is_effortless"), 1).otherwise(0)).as("data_7d_feedback_is_effortless"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("data_7d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15, 1).otherwise(0)).as("data_15d_close"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("has_escalation"), 1).otherwise(0)).as("data_15d_escalation"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("feedback_is_effortless"), 1).otherwise(0)).as("data_15d_feedback_is_effortless"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("data_15d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>15, 1).otherwise(0)).as("data_15d_more_close"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>15 && col("has_escalation"), 1).otherwise(0)).as("data_15d_more_escalation"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>15 && col("feedback_is_effortless"), 1).otherwise(0)).as("data_15d_more_feedback_is_effortless"),
               sum(when(col("shard")==="Data" && col("case_status_shortened")==="C" && col("case_age")>15 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("data_15d_more_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Infra", 1).otherwise(0)).as("infra_case_count"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")<=3, 1).otherwise(0)).as("infra_3d_close"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("has_escalation"), 1).otherwise(0)).as("infra_3d_escalation"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("feedback_is_effortless"), 1).otherwise(0)).as("infra_3d_feedback_is_effortless"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("infra_3d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7, 1).otherwise(0)).as("infra_7d_close"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("has_escalation"), 1).otherwise(0)).as("infra_7d_escalation"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("feedback_is_effortless"), 1).otherwise(0)).as("infra_7d_feedback_is_effortless"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("infra_7d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15, 1).otherwise(0)).as("infra_15d_close"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("has_escalation"), 1).otherwise(0)).as("infra_15d_escalation"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("feedback_is_effortless"), 1).otherwise(0)).as("infra_15d_feedback_is_effortless"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("infra_15d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>15, 1).otherwise(0)).as("infra_15d_more_close"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>15 && col("has_escalation"), 1).otherwise(0)).as("infra_15d_more_escalation"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>15 && col("feedback_is_effortless"), 1).otherwise(0)).as("infra_15d_more_feedback_is_effortless"),
               sum(when(col("shard")==="Infra" && col("case_status_shortened")==="C" && col("case_age")>15 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("infra_15d_more_feedback_is_satisfied_support_rep"),


               sum(when(col("shard")==="Networking", 1).otherwise(0)).as("networking_case_count"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")<=3, 1).otherwise(0)).as("networking_3d_close"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("has_escalation"), 1).otherwise(0)).as("networking_3d_escalation"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("feedback_is_effortless"), 1).otherwise(0)).as("networking_3d_feedback_is_effortless"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("networking_3d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7, 1).otherwise(0)).as("networking_7d_close"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("has_escalation"), 1).otherwise(0)).as("networking_7d_escalation"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("feedback_is_effortless"), 1).otherwise(0)).as("networking_7d_feedback_is_effortless"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("networking_7d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15, 1).otherwise(0)).as("networking_15d_close"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("has_escalation"), 1).otherwise(0)).as("networking_15d_escalation"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("feedback_is_effortless"), 1).otherwise(0)).as("networking_15d_feedback_is_effortless"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("networking_15d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>15, 1).otherwise(0)).as("networking_15d_more_close"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>15 && col("has_escalation"), 1).otherwise(0)).as("networking_15d_more_escalation"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>15 && col("feedback_is_effortless"), 1).otherwise(0)).as("networking_15d_more_feedback_is_effortless"),
               sum(when(col("shard")==="Networking" && col("case_status_shortened")==="C" && col("case_age")>15 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("networking_15d_more_feedback_is_satisfied_support_rep"),


               sum(when(col("shard")==="Platform", 1).otherwise(0)).as("platform_case_count"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")<=3, 1).otherwise(0)).as("platform_3d_close"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("has_escalation"), 1).otherwise(0)).as("platform_3d_escalation"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("feedback_is_effortless"), 1).otherwise(0)).as("platform_3d_feedback_is_effortless"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")<=3 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("platform_3d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7, 1).otherwise(0)).as("platform_7d_close"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("has_escalation"), 1).otherwise(0)).as("platform_7d_escalation"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("feedback_is_effortless"), 1).otherwise(0)).as("platform_7d_feedback_is_effortless"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>3 && col("case_age")<=7 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("platform_7d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15, 1).otherwise(0)).as("platform_15d_close"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("has_escalation"), 1).otherwise(0)).as("platform_15d_escalation"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("feedback_is_effortless"), 1).otherwise(0)).as("platform_15d_feedback_is_effortless"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>7 && col("case_age")<=15 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("platform_15d_feedback_is_satisfied_support_rep"),

               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>15, 1).otherwise(0)).as("platform_15d_more_close"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>15 && col("has_escalation"), 1).otherwise(0)).as("platform_15d_more_escalation"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>15 && col("feedback_is_effortless"), 1).otherwise(0)).as("platform_15d_more_feedback_is_effortless"),
               sum(when(col("shard")==="Platform" && col("case_status_shortened")==="C" && col("case_age")>15 && col("feedback_is_satisfied_support_rep"), 1).otherwise(0)).as("platform_15d_more_feedback_is_satisfied_support_rep"),
            )
          .select("customer_id", "customer_name", "industry", "customer_is_hypercare",
            "total_case_count",
            "data_case_count",
            "data_3d_close",
            "data_3d_escalation",
            "data_3d_feedback_is_effortless",
            "data_3d_feedback_is_satisfied_support_rep",

            "data_7d_close",
            "data_7d_escalation",
            "data_7d_feedback_is_effortless",
            "data_7d_feedback_is_satisfied_support_rep",

            "data_15d_close",
            "data_15d_escalation",
            "data_15d_feedback_is_effortless",
            "data_15d_feedback_is_satisfied_support_rep",

            "data_15d_more_close",
            "data_15d_more_escalation",
            "data_15d_more_feedback_is_effortless",
            "data_15d_more_feedback_is_satisfied_support_rep",

            "infra_case_count",
            "infra_3d_close",
            "infra_3d_escalation",
            "infra_3d_feedback_is_effortless",
            "infra_3d_feedback_is_satisfied_support_rep",

            "infra_7d_close",
            "infra_7d_escalation",
            "infra_7d_feedback_is_effortless",
            "infra_7d_feedback_is_satisfied_support_rep",

            "infra_15d_close",
            "infra_15d_escalation",
            "infra_15d_feedback_is_effortless",
            "infra_15d_feedback_is_satisfied_support_rep",

            "infra_15d_more_close",
            "infra_15d_more_escalation",
            "infra_15d_more_feedback_is_effortless",
            "infra_15d_more_feedback_is_satisfied_support_rep",

            "networking_case_count",
            "networking_3d_close",
            "networking_3d_escalation",
            "networking_3d_feedback_is_effortless",
            "networking_3d_feedback_is_satisfied_support_rep",

            "networking_7d_close",
            "networking_7d_escalation",
            "networking_7d_feedback_is_effortless",
            "networking_7d_feedback_is_satisfied_support_rep",

            "networking_15d_close",
            "networking_15d_escalation",
            "networking_15d_feedback_is_effortless",
            "networking_15d_feedback_is_satisfied_support_rep",

            "networking_15d_more_close",
            "networking_15d_more_escalation",
            "networking_15d_more_feedback_is_effortless",
            "networking_15d_more_feedback_is_satisfied_support_rep",

            "platform_case_count",
            "platform_3d_close",
            "platform_3d_escalation",
            "platform_3d_feedback_is_effortless",
            "platform_3d_feedback_is_satisfied_support_rep",

            "platform_7d_close",
            "platform_7d_escalation",
            "platform_7d_feedback_is_effortless",
            "platform_7d_feedback_is_satisfied_support_rep",

            "platform_15d_close",
            "platform_15d_escalation",
            "platform_15d_feedback_is_effortless",
            "platform_15d_feedback_is_satisfied_support_rep",

            "platform_15d_more_close",
            "platform_15d_more_escalation",
            "platform_15d_more_feedback_is_effortless",
            "platform_15d_more_feedback_is_satisfied_support_rep"
            )
          .show();
        // df.printSchema();
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
