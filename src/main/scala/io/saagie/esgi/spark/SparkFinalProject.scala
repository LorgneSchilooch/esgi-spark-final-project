package io.saagie.esgi.spark

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._


object SparkFinalProject {

var path_file = ""

  def main(args: Array[String]) {
    println(s"Sleeping for ${args(0)}")

    if (args(1) == "local") {
   path_file += "./data"
    } else {
     path_file += "hdfs:/data/esgi-spark/final-project"
    }
    val str_reviews = path_file + "/reviews.csv"
    val str_beers = path_file + "/beers.csv"
    val str_breweries = path_file + "/breweries.csv"
    val str_stopword = path_file + "/stopword.csv"


    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val df_reviews = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(str_reviews)
    val df_beers = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(str_beers)
    val df_breweries = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(str_breweries)
    val df_stopwords = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(str_stopword)


    val score = df_reviews
      .groupBy("beer_id")
      .agg(avg("score") as "moy", count("username") as "count")

    val joined_df = score
      .join(df_beers, col("beer_id") === col("id"), "left")
      .orderBy(desc("moy"), desc("count"))

    println(joined_df.show(1))


    val score_brew = joined_df
      .groupBy("brewery_id")
      .agg(avg("moy") as "brew_moy", count("retired") as "count_brew")

    val best_brew = score_brew
      .join(df_breweries, col("brewery_id") === col("id"), "left")
      .orderBy(desc("brew_moy"), desc("count_brew"))

    println(best_brew.show(1))


    val most_country = df_breweries
      .where(col("types") contains "Beer-to-go")
      .groupBy("country")
      .agg(count("country") as "count_country")
      .orderBy(desc("count_country"))

    println(most_country.show(10))


    val filter_IPA_beers = df_beers
      .where(col("style") contains "IPA")

    val join_IPA = filter_IPA_beers
      .join(df_reviews, col("beer_id") === col("id"), "left")


    val word_use = join_IPA
      .select(col("text"))
      .where(col("text") isNotNull)
      .explode("text", "word")((line: String) => line.split(" "))
      .groupBy("word")
      .agg(count("word") as "count_word")
      .orderBy(desc("count_word"))
    println(word_use.show(10))


    Thread.sleep(args(0).toLong)
  }

}
