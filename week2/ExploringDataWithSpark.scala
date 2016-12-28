
object Cells {
  //define sql context
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
  
  // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
  import sqlContext.implicits._

  /* ... new cell ... */

  val spotCheckingData = sqlContext.read.option("quote","\"").option("header","false").option("escape","\"").csv("/home/indix/repos/mlbootcamp/week2/spot_check_data.csv.bz2")

  /* ... new cell ... */

  spotCheckingData.take(2)

  /* ... new cell ... */

  val requiredColumns = List("title", "breadCrumbs", "categoryNamePath", "categoryNamePath_status", "title_status", "breadCrumbs_status")
  val columns = spotCheckingData.head().toSeq.map(x => if (x == null) "null" else x.toString).zipWithIndex
  .filter(x => requiredColumns.contains(x._1)).map(x => ("_c" + (x._2), x._1)).toList

  /* ... new cell ... */

  val cols = columns.map(x => x._1)
  val cols2 = columns.map(x => x._2)
  val spotCheckData = spotCheckingData.select(cols.head, cols.tail: _*).toDF(cols2: _*).filter($"_c26" !== "title")
  spotCheckData.show()

  /* ... new cell ... */

  import org.apache.spark.sql.functions.udf
  def wordCount(str:String) = {
    str.split(" ").filter(_.nonEmpty).length
  }
  val wordCountUDF = udf(wordCount _)
  val titleWithWordCount = spotCheckData.select("title").withColumn("wordCount", wordCountUDF($"title"))

  /* ... new cell ... */

  titleWithWordCount.select("wordCount").collect()

  /* ... new cell ... */

  def wordCount(str:String) = {
    if(str != null) str.split(" ").filter(_.nonEmpty).filter(x => x != ">").length else 0
  }
  val wordCountUDF = udf(wordCount _)
  val breadCrumbsWithWordCount = spotCheckData.select("breadCrumbs").withColumn("wordCount", wordCountUDF($"breadCrumbs"))

  /* ... new cell ... */

  breadCrumbsWithWordCount.select("wordCount").collect()

  /* ... new cell ... */

  val clfnData = spotCheckData.select("categoryNamePath", "categoryNamePath_status").filter(not(isnull($"categoryNamePath_status")))
  val clfnSpotCheckGroup = clfnData.groupBy("categoryNamePath_status").agg(count($"categoryNamePath_status")).collect().map(_.toSeq).map(x => (x(0), x(1).asInstanceOf[Long])).toMap

  /* ... new cell ... */

  clfnSpotCheckGroup("Incorrect")

  /* ... new cell ... */

  clfnSpotCheckGroup("Incorrect") * 1.0 / (clfnSpotCheckGroup("Incorrect") + clfnSpotCheckGroup("Correct"))

  /* ... new cell ... */

  val bdcb_clfn_data = spotCheckData.select("categoryNamePath", "categoryNamePath_status", "breadCrumbs", "breadCrumbs_status")
  val bdcb_invalid_clfn_correct_data = bdcb_clfn_data.filter(not(isnull($"categoryNamePath_status"))).filter(not(isnull($"breadCrumbs_status")))
  .filter($"categoryNamePath_status" === "Incorrect").filter($"breadCrumbs_status" =!= "Correct")
  
  bdcb_invalid_clfn_correct_data.count()

  /* ... new cell ... */

  val bdcb_value_counts = bdcb_clfn_data.groupBy("breadCrumbs_status").agg(count($"breadCrumbs_status")).collect().map(_.toSeq).map(x => (x(0), x(1).asInstanceOf[Long])).toMap

  /* ... new cell ... */

  (bdcb_value_counts.values.sum - bdcb_value_counts("Correct")) * 1.0 / bdcb_value_counts.values.sum  

  /* ... new cell ... */

  val bdcb_invalid_clfn_correct_data = bdcb_clfn_data.filter(not(isnull($"categoryNamePath_status"))).filter(not(isnull($"breadCrumbs_status")))
  .filter($"categoryNamePath_status" === "Correct").filter($"breadCrumbs_status" =!= "Correct")
  val bdcb_invalid_clfn_correct_count = bdcb_invalid_clfn_correct_data.count()

  /* ... new cell ... */

  bdcb_invalid_clfn_correct_count * 1.0 / (bdcb_value_counts.values.sum - bdcb_value_counts("Correct"))

  /* ... new cell ... */

  def topCategory(category:String) = {
    if(category != null) category.split(" >").head else ""
  }
  val topCategoryUDF = udf(topCategory _)
  val cathPathWithTopCategory = bdcb_invalid_clfn_correct_data.select("categoryNamePath").withColumn("topCategory", topCategoryUDF($"categoryNamePath"))

  /* ... new cell ... */

  cathPathWithTopCategory.groupBy($"topCategory").agg(count($"topCategory").alias("topCatCount")).sort(desc("topCatCount"))

  /* ... new cell ... */
}
                  