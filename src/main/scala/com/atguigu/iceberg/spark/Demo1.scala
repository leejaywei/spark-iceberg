package com.atguigu.iceberg.spark

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * TODO
  *
  * @version 1.0
  * @author cjp
  *
  */
object Demo1 {
  def main( args: Array[String] ): Unit = {

    // 1.创建 Catalog
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName)
      //指定hive catalog, catalog名称为iceberg_hive
      .config("spark.sql.catalog.iceberg_hive", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_hive.type", "hive")
      .config("spark.sql.catalog.iceberg_hive.uri", "thrift://hadoop1:9083")
      .config("iceberg.engine.hive.enabled", "true")
      //指定hadoop catalog，catalog名称为iceberg_hadoop
      .config("spark.sql.catalog.iceberg_hadoop", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_hadoop.type", "hadoop")
      .config("spark.sql.catalog.iceberg_hadoop.warehouse", "hdfs://hadoop1:8020/warehouse/spark-iceberg")
      .getOrCreate()

    // 2.读取表
//    spark.read
//      .format("iceberg")
//      .option("snapshot-id",7601163594701794741L) // 指定快照id查询
//      .load("hdfs://hadoop1:8020/warehouse/spark-iceberg/default/a")
//      .show()

//    spark.table("iceberg_hadoop.default.a").show() //spark3才支持


    // 3.检查表
//    spark.read.format("iceberg")
////      .option("snapshot-id",7601163594701794741L)
//      //      .load("hdfs://hadoop1:8020/warehouse/spark-iceberg/default/a#files")
//            .load("iceberg_hadoop.default.a.files")
////      .load("iceberg_hadoop.default.a.snapshots")
////      .load("iceberg_hadoop.default.a.history")
////      .load("iceberg_hadoop.default.a.manifests")
//      .show()

    // 4.写入表
    val df: DataFrame = spark.createDataFrame(Seq(Sample(1,"A","a"),Sample(2,"B","b"),Sample(3,"C","c")))
//    df.writeTo("iceberg_hadoop.default.table1").create() // 插入并建表
//    df.writeTo("iceberg_hadoop.default.table1").append()  // 追加写入

    // 插入并修改表结构
//    import spark.implicits._
//    df.writeTo("iceberg_hadoop.default.table1")
//      .tableProperty("write.format.default","orc")
//      .partitionedBy($"category")
//      .createOrReplace()

//    val df1: DataFrame = spark.createDataFrame(Seq(Sample(11,"A1","a1"),Sample(22,"B2","b2"),Sample(333,"C33","c3")))
//
//    import spark.implicits._
//    df1.writeTo("iceberg_hadoop.default.table1")
////      .overwritePartitions()
//      .overwrite($"category" === "c3")


    df.sortWithinPartitions("category")
      .writeTo("iceberg_hadoop.default.table1")
      .append()

  }

  case class Sample(id:Int,data:String,category:String)

}
