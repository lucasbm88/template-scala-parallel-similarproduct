package org.template.similarproduct

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger
import org.apache.spark.sql.hive.HiveContext

import io.prediction.controller.SanityCheck

case class DataSourceParams(appName: String, startDate: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  def whileLoop(condition: => Boolean)(command: => Unit) {
    if (condition) {
      command; whileLoop(condition)(command)
    } else ()
  }


  override
  def readTraining(sc: SparkContext): TrainingData = {

    logger.info(s"___Changing data to HiveContext___")


    val sqlContext = new HiveContext(sc)
    val comSaleOrderDf = sqlContext.sql("select salord_id_account_buyer, salord_id_product, " +
      "unix_timestamp(salord_date_sale_order) from core.com_sale_order where to_date(salord_date_sale_order) > to_date("+dsp.startDate+")")

    // create a RDD of (entityID, User)
    logger.info(s"___Creating User RDDS___")
    val usersRDD: RDD[(String, User)] = comSaleOrderDf.map { case row =>
      val user = try {
        User()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties of" +
            s" user ${row.getString(0)}. Exception: ${e}.")
          throw e
        }
      }
      (row.getString(0), user)
    }


    // create a RDD of (entityID, Item)
    logger.info(s"___[DS]Creating item RDDs___")
    val itemsRDD: RDD[(String, Item)] = comSaleOrderDf.map { case row =>
      val item = try{
        Item(categories = Option[List[String]](List("")))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties of item ${row.get(1)}. Exception: ${e}.")
          throw e
        }
      }
      (row.getString(1), item)
    }
    
    logger.info(s"___[DS]Creating ViewEvent RDD___")
    val viewEventsRDD: RDD[ViewEvent] = comSaleOrderDf.map { case row =>
        val viewEvent = try{
          ViewEvent(user = row.getString(0),
                            item = row.getString(1),
                            t = row.getLong(2))
        } catch {
          case e: Exception => {
            logger.error(s"Cannot convert viewEvent. Exception: ${e}")
            throw e
          }
        }
        viewEvent
    }

    logger.info(s"___[DS]Creating new TrainingData___")
    new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      viewEvents = viewEventsRDD
    )
  }
}

case class User()

case class Item(categories: Option[List[String]])

case class ViewEvent(user: String, item: String, t: Long)

//case class BuyEvent(user: String, item: String, t: Long)

class TrainingData (
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val viewEvents: RDD[ViewEvent]
  ) extends Serializable with SanityCheck{

  override def toString = {
      s"users: [${users.count()} (${users.take(2).toList}...)]" +
      s"items: [${items.count()} (${items.take(2).toList}...)]" +
      s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)"
  }

  override def sanityCheck(): Unit = {
    println(toString())

    @transient lazy val logger = Logger[this.type]

  }
}
