package com.atguigu.iceberg.spark

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Table
import org.apache.iceberg.actions.RewriteDataFiles
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.SparkSession

/**
  * TODO
  *
  * @version 1.0
  * @author cjp
  *
  */
object Demo2 {
  def main( args: Array[String] ): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getSimpleName).getOrCreate()

    // 1.创建catalog对象
    // 1.1 hadoop catalog
    val conf = new Configuration()
    val catalog = new HadoopCatalog(conf,"hdfs://hadoop1:8020/warehouse/spark-iceberg")

    // 1.2 hive catalog
//    val hiveCatalog = new HiveCatalog
//    val properties = new util.HashMap[String,String]()
//    properties.put("uri","thrift://hadoop1:9083")
//    hiveCatalog.initialize("hive",properties)

    // 2.通过catalog获取table对象
    val table: Table = catalog.loadTable(TableIdentifier.of("default","table1"))

//    val table: Table = hiveCatalog.loadTable(TableIdentifier.of("default","table1"))

    val tsToExpire: Long = System.currentTimeMillis() - 10*1000L


    // 3. 过期快照清理
//    table.expireSnapshots()
//      .expireOlderThan(tsToExpire)
//      .commit()


//    SparkActions.get()
//      .expireSnapshots(table)
//      .expireOlderThan(tsToExpire)
//      .execute()

    // 4. 清理无效文件
//    SparkActions.get()
    //      .deleteOrphanFiles(table)
    //      .execute()


    // 5. 合并小文件
    val table1: Table = catalog.loadTable(TableIdentifier.of("default","a"))
    val result: RewriteDataFiles.Result = SparkActions.get()
      .rewriteDataFiles(table1)
      //      .filter(Expressions.equal("category","a"))
      .option("target-file-size-bytes", 1024L.toString)
      .execute()
    println(result.rewrittenDataFilesCount())



  }
}
