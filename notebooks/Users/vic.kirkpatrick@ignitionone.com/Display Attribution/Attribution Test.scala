// Databricks notebook source
// MAGIC %run "/Shared/DataCore/Utils"

// COMMAND ----------

/*
Display Attribution

Notebook parameters:
advertiserId: distinct advertiser_id or * for all
attributionLookBackWindow: should be 7 days unless instructed by Product to change. This will affect lookback window on Impressions and Clicks.
conversionLookBackWindow: should be 1 day unless instructed by Product to change. This will affect lookback window on Conversions only.
referenceDate: leave blank or change to current day if wanting to run for for the current day. Will start lookbacks from this date if wanting to start in the past.
*/

/*
NOTES / TODOs:
We do not have conversion_pixel_id in fact_conversions_nan, and it is used to partition the potential impressions, but is it conversion_type_id ???
Implement referenceDate so we can run attribution starting from any date in the past
When you get impressions, do so ONLY for the distinct advertisers we had conversions for
*/

// Use dbutils.widgets.text("name", "defaultStringVal", "label") to create text param widgets
//dbutils.widgets.text("advertiserId", "*", "advertiserId")
//dbutils.widgets.text("conversionLookBackWindow", "*", "conversionLookBackWindow")
//dbutils.widgets.text("attributionLookBackWindow", "*", "attributionLookBackWindow")
//dbutils.widgets.text("referenceDate", "20180228", "referenceDate")
//dbutils.widgets.remove("conversionLookBackWindow")
import java.time.LocalDate
import scala.util.Try
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

def getReferenceDateParam(x: Any): LocalDate = x match {
    case y: String if Try(y.toInt).isSuccess && y.length == 8 => LocalDate.of(y.toInt / 10000 , y.toInt/100 % 100, y.toInt % 100  )
    case _ => LocalDate.now()
}

val referenceDate = getReferenceDateParam(dbutils.widgets.get("referenceDate")) // Must be in YYYYMMDD format
val advertiserIdArg = dbutils.widgets.get("advertiserId")
val conversionLookBackWindowArg = dbutils.widgets.get("conversionLookBackWindow").toInt
val attributionLookBackWindowArg = dbutils.widgets.get("attributionLookBackWindow").toInt
val conversionsNanPath = s"/mnt/display-analytics/fact_conversions_nan_v3/advertiser_id_part=${advertiserIdArg}/conversion_date_part="
val browserClicksPath = s"/mnt/display-analytics/fact_clicks_v3/advertiser_id_part=${advertiserIdArg}/click_date_part="
val browserImpressionsPath = s"/mnt/display-analytics/fact_winnotifications_v4/advertiser_id_part=${advertiserIdArg}/impression_date_part="
val appClicksPath = s"/mnt/display-analytics/fact_clicks_mobile_raw/click_date_local_part="
val appImpressionsPath = s"/mnt/display-analytics/fact_winnotifications_mobile_v2/advertiser_id_part=${advertiserIdArg}/impression_date_part="

// COMMAND ----------

import java.time.LocalDate

def getDays(fromDate: LocalDate, toDate: LocalDate) = {
  fromDate.toEpochDay.until(toDate.toEpochDay).map(LocalDate.ofEpochDay)
}

// Conversion window is the run date minus the Conversion Lookback Window, which is typically just 1 day, making it such that we run for both yesterday and today

val startDate = referenceDate.plusDays(conversionLookBackWindowArg.abs * -1)
val convDates = getDays(startDate, startDate.plusDays(2)).map(_.toString.replaceAll("[-]", ""))
val convDatesString = convDates.mkString(",")

val conversionsNanFiles = s"$conversionsNanPath{$convDatesString}"

// COMMAND ----------

import java.time.LocalDate

// Impression & Clicks window is the run date minus the Attribution Lookback Window, which is typically just 7 days.

val startDate = referenceDate.plusDays(attributionLookBackWindowArg.abs * -1)
val rangeDates = getDays(startDate, startDate.plusDays(attributionLookBackWindowArg+1)).map(_.toString.replaceAll("[-]", "")) // remove dashes for other folders
val rangeDatesString = rangeDates.mkString(",")

val browserImpressionsPathPattern = s"$browserImpressionsPath{$rangeDatesString}"
val browserClicksPathPattern = s"$browserClicksPath{$rangeDatesString}"
val appImpressionsPathPattern = s"$appImpressionsPath{$rangeDatesString}"
val appClicksPathPattern = s"$appClicksPath{$rangeDatesString}"

// COMMAND ----------

val RevenueConfig = spark
  .read
  .csv("/mnt/ione-prod-bireporting/prod/display/ione/revenue_config_nan/")
  .withColumnRenamed("_c0", "advertiser_id")
  .withColumnRenamed("_c1", "rev_config_name")

display(RevenueConfig)

// COMMAND ----------

val revUDF = udf(RevenueUtils.getRevenueFromConversionParams _)

val dimDates = spark
  .read
  .parquet("/mnt/display-analytics/l_dates")
  .select(
          'date_id,
          'date,
          'smartDate
         )

val ConversionsNan = spark
  .read
  .parquet(conversionsNanFiles)
  .select(
            'advertiser_id,
            'conversion_id,
            'visitor_id.alias("conv_visitor_id"),
            'uvisitor_id.alias("conv_uvisitor_id"),
            'visit_id.alias("conv_visit_id"),
            'date_id.alias("conv_date_id"),
            'conversion_date,
            'conversion_ts,
            'conversion_type_id,
            'p1,'p2,'p3,'p4,'p5,'p6,'p7,'p8,'p9,'p10,
            'country_code,
            'metro_code,
            'city_code
         )
//display(ConversionsNan)
//display(ConversionsNan.limit(100))

val ConversionsNanRevenue = ConversionsNan
  .join(RevenueConfig, Seq("advertiser_id"))
  .join(dimDates,
         ConversionsNan.col("conv_date_id") === dimDates.col("date_id")
       )
  .withColumn("revenue", revUDF(col("advertiser_id"),RevenueConfig.col("rev_config_name"),ConversionsNan.col("p1"),ConversionsNan.col("p2"),ConversionsNan.col("p3"),ConversionsNan.col("p4"),ConversionsNan.col("p5")))
  .select(
            'advertiser_id,
            'revenue,
            'conversion_id,
            'conv_visitor_id,
            'conv_uvisitor_id,
            'conv_visit_id,
            'conv_date_id,
            'smartDate.alias("conversion_date_local").cast("Integer"),
            'conversion_date,
            'conversion_ts,
            'conversion_type_id,
            'p1,'p2,'p3,'p4,'p5,'p6,'p7,'p8,'p9,'p10,
            'country_code,
            'metro_code,
            'city_code,
            'smartDate.alias("conversion_date_local_part").cast("Integer")
         )

display(ConversionsNanRevenue)

// Get distinct Advertisers for those Conversions
val ConversionsAdvertisers = ConversionsNan
  .select('advertiser_id)
  .distinct()

//display(ConversionsAdvertisers)

// COMMAND ----------

// Do not allow date override here. Should be same window as the main attribution window.
// Group to Impression, taking min click datetime

val BrowserClicks = spark
  .read
  .parquet(browserClicksPathPattern)
  .groupBy('impression_id)
  .agg(min('click_ts).alias("click_ts"))
  .orderBy("impression_id")

//display(BrowserClicks)
//BrowserClicks.count


// COMMAND ----------

val AppClicks = spark
  .read
  .parquet(appClicksPathPattern)
  .select(
           'businessunit_id,
           'click_id,
           'click_ts,
           'click_date,
           'u_visitor.alias("click_u_visitor_id"),
           'impression_id
         )

//display(AppClicks)

// COMMAND ----------

// Do not allow date override here. Should be same window as the main attribution window.

val BrowserImpressions = spark
  .read
  .parquet(browserImpressionsPathPattern)
  .select(
            lit("1").alias("impression_type"), // 1 = Browser; 2 = App
            'businessunit_id,
            'advertiser_id,
            'impression_id,
            'visitor_id.alias("imp_visitor_id"),
            'uvisitor_id.alias("imp_uvisitor_id"),
            'impression_date,
            'impression_date_local,
            'impression_ts,
            'date_id.alias("imp_date_id"),
            'publisher_id,
            'campaign_id,
            'creative_id,
            'nm_height,
            'nm_width,
            'category_id,
            'category_score,
            'category_age,
            'udger_browser_name,
            'udger_os_name,
            'udger_os_family_name,
            'udger_type_name,
            lit("").alias("app_name"), // Browser impressions may have a value here, but it is not the actual application name/id
            lit("").alias("app_id"),
            'url_raw,
            'url_host,
            'url_path,
            'raw_cost,
            'cost,
            'maxcpm
         )

//display(BrowserImpressions)
//BrowserImpressions.count

// COMMAND ----------

// Do not allow date override here. Should be same window as the main attribution window.

val AppImpressions = spark
  .read
  .parquet(appImpressionsPathPattern)
  .select(
            'businessunit_id,
            'advertiser_id,
            'impression_id,
            'visitor_id.alias("imp_visitor_id"),
            'uvisitor_id.alias("imp_uvisitor_id"),
            'impression_date,
            'impression_date_local,
            'impression_ts,
            'date_id,
            'publisher_id,
            'campaign_id,
            'creative_id,
            'nm_height,
            'nm_width,
            'category_id,
            'category_score,
            'category_age,
            'udger_browser_name,
            'udger_os_name,
            'udger_os_family_name,
            'udger_type_name,
            'app_name,
            'app_id,
            'url_raw,
            'url_host,
            'url_path,
            'raw_cost,
            'cost,
            'maxcpm
         )

//display(AppImpressions)
//AppImpressions.count

// COMMAND ----------

val AppImpressionsClicks = AppClicks
  .join(AppImpressions, Seq("impression_id"))
  .select(
            AppClicks.col("businessunit_id").alias("businessunit_id"),
            'advertiser_id.alias("imp_advertiser_id"),
    // click cols
            'click_id,
            'click_ts,
            'click_date,
            'click_u_visitor_id,
    //  impression cols
            'impression_id,
            'imp_visitor_id,
            'imp_uvisitor_id,
            'impression_date,
            'impression_date_local,
            'impression_ts,
            'date_id.alias("imp_date_id"),
            'publisher_id,
            'campaign_id,
            'creative_id,
            'nm_height,
            'nm_width,
            'category_id,
            'category_score,
            'category_age,
            'udger_browser_name,
            'udger_os_name,
            'udger_os_family_name,
            'udger_type_name,
            'app_name,
            'app_id,
            'url_raw,
            'url_host,
            'url_path,
            'raw_cost,
            'cost,
            'maxcpm
         )

//display(AppImpressionsClicks)

// COMMAND ----------

val ConversionsBrowserImpressions = ConversionsNanRevenue
  .join(BrowserImpressions, ConversionsNanRevenue.col("advertiser_id") === BrowserImpressions.col("advertiser_id")
                              && ConversionsNanRevenue.col("conv_visitor_id") === BrowserImpressions.col("imp_visitor_id")
       )
  .join(ConversionsAdvertisers, "advertiser_id")
  .join(BrowserClicks, Seq("impression_id"), "left_outer")
  .withColumn("click_based", when('click_ts === lit(""), "0").otherwise("1").cast("Boolean")) // for browser, do this logic, but for app it should always be click-based
  .select(
            'businessunit_id,
            ConversionsNanRevenue.col("advertiser_id"),
            'revenue,
            'conversion_id,
            'conv_visitor_id,
            'conv_uvisitor_id,
            'conv_visit_id,
            'conv_date_id,
            'conversion_date_local,
            'conversion_date,
            'conversion_ts,
            'conversion_type_id,
            lit("1").alias("impression_type"), // 1 = Browser; 2 = App
            'impression_id,
            'imp_visitor_id,
            'imp_uvisitor_id,
            'impression_date,
            'impression_date_local,
            'impression_ts,
            'imp_date_id,
            'click_based,
            'publisher_id,
            'campaign_id,
            'creative_id,
            'nm_height,
            'nm_width,
            'category_id,
            'category_score,
            'category_age,
            'udger_browser_name,
            'udger_os_name,
            'udger_os_family_name,
            'udger_type_name,
            'app_name,
            'app_id,
            'url_raw,
            'url_host,
            'url_path,
            'raw_cost,
            'cost,
            'maxcpm,
            'p1,'p2,'p3,'p4,'p5,'p6,'p7,'p8,'p9,'p10,
            'country_code,
            'metro_code,
            'city_code,
            'conversion_date_local_part
  )

//display(ConversionsBrowserImpressions)

// COMMAND ----------

val ConversionsAppImpressions = ConversionsNanRevenue
  .join(AppImpressionsClicks, ConversionsNanRevenue.col("advertiser_id") === AppImpressionsClicks.col("imp_advertiser_id")
                                && ConversionsNanRevenue.col("conv_uvisitor_id") === AppImpressionsClicks.col("click_u_visitor_id")
       )
  .join(ConversionsAdvertisers, "advertiser_id")
  .select(
            'businessunit_id,
            ConversionsNanRevenue.col("advertiser_id"),
            'revenue,
            'conversion_id,
            'conv_visitor_id,
            'conv_uvisitor_id,
            'conv_visit_id,
            'conv_date_id,
            'conversion_date_local,
            'conversion_date,
            'conversion_ts,
            'conversion_type_id,
            lit("2").alias("impression_type"), // 1 = Browser; 2 = App
            'impression_id,
            'imp_visitor_id,
            'click_u_visitor_id.alias("imp_uvisitor_id"),
            'impression_date,
            'impression_date_local,
            'impression_ts,
            'imp_date_id,
            lit("1").cast("Boolean").alias("click_based"), // All app impressions are click-based by nature
            'publisher_id,
            'campaign_id,
            'creative_id,
            'nm_height,
            'nm_width,
            'category_id,
            'category_score,
            'category_age,
            'udger_browser_name,
            'udger_os_name,
            'udger_os_family_name,
            'udger_type_name,
            'app_name,
            'app_id,
            'url_raw,
            'url_host,
            'url_path,
            'raw_cost,
            'cost,
            'maxcpm,
            'p1,'p2,'p3,'p4,'p5,'p6,'p7,'p8,'p9,'p10,
            'country_code,
            'metro_code,
            'city_code,
            'conversion_date_local_part
         )

//display(ConversionsAppImpressions)

// COMMAND ----------

val AllConversionsImpressions = ConversionsBrowserImpressions
  .union(ConversionsAppImpressions)

//display(AllConversionsImpressions)
//display(AllConversionsImpressions.limit(100))

// COMMAND ----------

val DisplayExposureSequence = AllConversionsImpressions
  .withColumn("row_num", row_number().over(Window.partitionBy(AllConversionsImpressions.col("advertiser_id"), AllConversionsImpressions.col(/*CHANGE ME TO CONVERSION_TYPE_ID ????*/"conversion_id")).orderBy(AllConversionsImpressions.col("impression_ts").desc, AllConversionsImpressions.col("impression_id"))))
  .filter("row_num = 1")
  .drop("row_num")
  .select(
//            'row_num,
            'advertiser_id,
            'businessunit_id,
            'revenue,
            'conversion_id,
            'conv_visitor_id,
            'conv_uvisitor_id,
            'conv_visit_id,
            'conv_date_id,
            'conversion_date_local,
            'conversion_date,
            'conversion_ts,
            'conversion_type_id,
            'impression_type,
            'impression_id,
            'imp_visitor_id,
            'imp_uvisitor_id,
            'impression_date,
            'impression_date_local,
            'impression_ts,
            'imp_date_id,
            'click_based,
            'publisher_id,
            'campaign_id,
            'creative_id,
            'nm_height,
            'nm_width,
            'category_id,
            'category_score,
            'category_age,
            'udger_browser_name,
            'udger_os_name,
            'udger_os_family_name,
            'udger_type_name,
            'app_name,
            'app_id,
            'url_raw,
            'url_host,
            'url_path,
            'raw_cost,
            'cost,
            'maxcpm,
            'p1,'p2,'p3,'p4,'p5,'p6,'p7,'p8,'p9,'p10,
            'country_code,
            'metro_code,
            'city_code,
            'advertiser_id.alias("advertiser_id_part"),
            'conversion_date_local_part
         )
//  .orderBy(AllConversionsImpressions.col("advertiser_id"), AllConversionsImpressions.col("CONVERSION_TYPE_ID? - was conversion_id"), AllConversionsImpressions.col("impression_ts").desc)

//DisplayExposureSequence.printSchema()

display(DisplayExposureSequence)

// COMMAND ----------

//val outputFolder = "/mnt/bm-common/output/dev/AttributionTest"
val outputFolder = s"/mnt/ione-prod-bireporting/staging/display/ione/display_exposure_sequence/"

DisplayExposureSequence
  .write
  .mode(SaveMode.Overwrite)
  .option("compression", "none")
//  .partitionBy("advertiser_id")
  .partitionBy("conversion_date_local_part")
  .parquet(outputFolder)