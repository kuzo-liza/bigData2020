import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameStatFunctions
import org.apache.spark.sql.expressions.Window

object hw1 extends App {

  val spark = SparkSession
    .builder()
    .appName("kuzovkina")
    .master("local")
    .getOrCreate()

    val data = spark
      .read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("escape", "\"")
      .csv("/Users/lizakuzovkina/Technopolis/bigData2020/hw0/kuzovkina/project/resourses/AB_NYC_2019.csv")


  // 1. Посчитать медиану, моду и среднее и дисперсию для каждого room_type

  data.createOrReplaceTempView("data")

  spark.sql("select room_type, percentile_approx(price, 0.5) as median from data group by room_type").show()

  data
    .groupBy("room_type", "price")
    .count()
    .withColumn("row_number", row_number().over(Window.partitionBy("room_type").orderBy(desc("count"))))
    .select("room_type", "price")
    .where(col("row_number") === 1)
    .show()

  data
    .groupBy("room_type")
    .agg(avg("price"))
    .show

  data
    .groupBy("room_type")
    .agg(variance("price"))
    .show()


  // 2. Найти самое дорогое и самое дешевое предложение

  data
    .orderBy(desc("price"))
    .show(1)

  data
    .orderBy("price")
    .where(col("price") > 0)
    .show(1)


  // 3. Посчитать корреляцию между ценой и минимальный количеством ночей, кол-вом отзывов

  spark.sql("select corr(price, minimum_nights) as correlationByNights from data").show()

  spark.sql("select corr(price, number_of_reviews) correlationByReviews from data").show()


  // 4.  Нужно найти гео квадрат размером 5км на 5км с самой высокой средней стоимостью жилья

  val encodeGeoHash = (lat: Double, lng: Double, precision: Int) => {
    val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    var (minLat, maxLat) = (-90.0, 90.0)
    var (minLng, maxLng) = (-180.0, 180.0)
    val bits = List(16, 8, 4, 2, 1)

    (0 until precision).map { p => {
      base32 apply (0 until 5).map { i => {
        if (((5 * p) + i) % 2 == 0) {
          val mid = (minLng + maxLng) / 2.0
          if (lng > mid) {
            minLng = mid
            bits(i)
          } else {
            maxLng = mid
            0
          }
        } else {
          val mid = (minLat + maxLat) / 2.0
          if (lat > mid) {
            minLat = mid
            bits(i)
          } else {
            maxLat = mid
            0
          }
        }
      }
      }.reduceLeft((a, b) => a | b)
    }
    }.mkString("")
  }

  val geoHash_udf = udf(encodeGeoHash)

  data
    .withColumn("geoHash", geoHash_udf(col("latitude"), col("longitude"), lit(5)))
    .groupBy("geoHash")
    .agg(avg("price"))
    .orderBy(desc("avg(price)"))
    .show(1)

}
