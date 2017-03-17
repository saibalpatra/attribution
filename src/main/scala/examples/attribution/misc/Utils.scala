package examples.attribution.misc

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object Utils {

  def getSession(): SparkSession = {
    val tmp = System.getProperty("java.io.tmpdir")
    val conf = new SparkConf()
    conf.set("spark.sql.warehouse.dir", tmp)
    SparkSession.builder().master("local").config(conf).getOrCreate()
  }

  /* Code from http://stackoverflow.com/questions/31674530/write-single-csv-file-using-spark-csv */
  def merge(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
    // the "true" setting deletes the source files once they are merged into the new output
  }

}