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

case class DataSourceParams(appName: String) extends Params

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

    val sqlContext = new HiveContext(sc)
    val comSaleOrderDf = sqlContext.sql("select salord_id_account_buyer, salord_id_product, unix_timestamp(salord_date_sale_order) from core.com_sale_order where to_date(salord_date_sale_order) > to_date('2014-03-01')")
    // create a RDD of (entityID, User)
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
    }.cache()


    // create a RDD of (entityID, Item)
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
    }.cache()

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
    }.cache()


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

class TrainingData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val viewEvents: RDD[ViewEvent]
) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
    s"items: [${items.count()} (${items.take(2).toList}...)]" +
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)"
  }
}
