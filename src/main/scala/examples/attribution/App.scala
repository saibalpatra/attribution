package examples.attribution

import examples.attribution.misc.Utils._
import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import examples.attribution.misc.Utils

/**
 * @author ${user.name}
 */
object App {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("App")

  def main(args: Array[String]) {

    val sc = Utils.getSession()

    val baseDir = args(0)
    
    val ep = new EventProcessor(sc, baseDir)

    val result = ep.process()

    import sc.sqlContext.implicits._

    /*
     * create the output file count_of_events.csv
     */
    var filename = "count_of_events.csv"
    var outputFileName = baseDir + "/temp_" + filename
    var mergedFileName = baseDir + "/" + filename
    var mergeFindGlob = outputFileName

    result._1.toDF().write
      .format("csv")
      .option("header", "false")
      .mode("overwrite")
      .save(outputFileName)
    merge(mergeFindGlob, mergedFileName)

    /*
     * create the output file count_of_users.csv
     */

    filename = "count_of_users.csv"
    outputFileName = baseDir + "/temp_" + filename
    mergedFileName = baseDir + "/" + filename
    mergeFindGlob = outputFileName

    result._2.toDF().write
      .format("csv")
      .option("header", "false")
      .mode("overwrite")
      .save(outputFileName)
    merge(mergeFindGlob, mergedFileName)

  }

}

trait RecordType {
  def timestamp: Int
  def advertiser_id: Int
  def user_id: String
  def record_type: String
}

case class Event(override val timestamp: Int, event_id: String, override val advertiser_id: Int, override val user_id: String, event_type: String, override val record_type: String = "event") extends RecordType

case class Impression(timestamp: Int, advertiser_id: Int, creative_id: Int, user_id: String, override val record_type: String = "impression") extends RecordType

class EventProcessor(sc: SparkSession, baseDir: String) {

  def process(): (RDD[(Int, String, Int)], RDD[(Int, String, Int)], RDD[((Int, String), Iterable[Event])], RDD[((Int, String), Iterable[(Int, String, String)])]) = {
    val sc = getSession()
    import sc.sqlContext.implicits._

    /* 
     * Read events.csv into a DataFrame, map into a dataset, and convert to RDD 
     */
    val events = sc.read.csv(baseDir + "events.csv")
      .map(r => { Event(r.getAs[String](0).toInt, r.getAs[String](1), r.getAs[String](2).toInt, r.getAs[String](3), r.getAs[String](4)) })
      .rdd

    /* 
     * group events by advertiser_id, user_id, event_type
     * process each group to remove duplicates 
     *  
     */
    val deDuplicatedEvents = events.groupBy { e => (e.advertiser_id, e.user_id, e.event_type) }
      .mapValues { group =>
        {

          var prevCandidateTimestamp: Int = 0;

          group.toList.sortBy { x => x.timestamp }.filter { x =>
            {
              var removeRecord = false

              if (x.timestamp - prevCandidateTimestamp < 60000) {
                removeRecord = false
              } else {
                removeRecord = true
              }

              prevCandidateTimestamp = x.timestamp
              removeRecord
            }
          }
        }
      }
      .values.flatMap(identity).asInstanceOf[RDD[RecordType]].map { r => ((r.advertiser_id, r.user_id), r) }

    /* 
     * Read impressions.csv into a DataFrame, map into a dataset, and convert to RDD 
     */
    val impressions = sc.read.csv(baseDir + "impressions.csv").map(
      r => Impression(r.getAs[String](0).toInt, r.getAs[String](1).toInt, r.getAs[String](2).toInt, r.getAs[String](3))).rdd.asInstanceOf[RDD[RecordType]].map { r => ((r.advertiser_id, r.user_id), r) }

    /* 
     * Merge deDuplicated Events and Impressions 
     * Sort the combined list by timestamp 
     * determine the attributed events
     */
    val attributedEvents = deDuplicatedEvents.cogroup(impressions).map(kv => {

      val sorted = (kv._2._1.toList ++ kv._2._2.toList).sortBy { x => x.timestamp }
      var impFound = false

      val attributedEventsSorted = sorted.filter { r =>
        {

          if (r.record_type == "impression") {
            impFound = true
            false
          } else {
            if (impFound && r.record_type == "event") {
              impFound = false
              true
            } else {
              false
            }
          }

        }
      }

      (attributedEventsSorted)

    }).flatMap(identity)

    /*attributedEvents.foreach(x => {
      val e = x.asInstanceOf[Event]; println(e.advertiser_id + ".." + e.user_id + "..." + e.event_id + ".." + e.event_type + ".." + e.timestamp)
    })*/

    /*
     * The count of attributed events for each advertiser, grouped by event type.
     */
    val eventsByAdvAndType = attributedEvents.asInstanceOf[RDD[Event]].groupBy(e => (e.advertiser_id, e.event_type))
    val CountOfAttributedEvents = eventsByAdvAndType.map(e => (e._1._1, e._1._2, e._2.size))

    /*
     * The count of unique users that have generated attributed events for each advertiser, grouped by event type.
     */
    val eventsByAdvAndTypeAndUser = attributedEvents.asInstanceOf[RDD[Event]].groupBy(e => (e.advertiser_id, e.event_type, e.user_id)).keys.groupBy(e => (e._1, e._2))
    val CountOfUniqueUsers = eventsByAdvAndTypeAndUser.map(e => (e._1._1, e._1._2, e._2.size))

    (CountOfAttributedEvents, CountOfUniqueUsers, eventsByAdvAndType, eventsByAdvAndTypeAndUser)

  }

}
