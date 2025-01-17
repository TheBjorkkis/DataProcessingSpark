package com.testscripts.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

/** Keep track of the most popular Twitter tweets
 *  hashtags over a 5 minute window.
 */
object PopularHashtags {
  
    /** Make sure only Errors are logged to avoid spam */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
  /** Configure Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source
    for (line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt file.
    setupTwitter()
    
    // Set up a Spark streaming context named "PopularHashtags" that runs locally.
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    // Get rid of log spam.
    setupLogging()

    // Create a DStream from Twitter using our streaming context.
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into DStreams using map().
    val statuses = tweets.map(status => status.getText())
    
    // Blow out each word into a new DStream.
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    // Now eliminate anything that's not a hashtag.
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    
    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values.
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    
    // Now count them up over a 5 minute window sliding every one second.
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    
    // Sort the results by the count values.
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // Print the top 10 hashtags at said time.
    sortedResults.print
    
    // Profit.
    ssc.checkpoint("C:/CheckpointLog/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
