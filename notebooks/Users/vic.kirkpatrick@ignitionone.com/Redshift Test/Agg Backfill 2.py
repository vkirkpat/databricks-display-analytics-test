# Databricks notebook source
import psycopg2
# if dbutils.widgets.get("redshiftZone") == 'prod':
#   hostname='bi-1d-redshift1-g4k9wlebzkvp.cifk98vup2ih.us-east-1.redshift.amazonaws.com'
# else:
#hostname='bi-redshift-prod-restore.cqye2sifwrao.us-east-1.redshift.amazonaws.com'
hostname='bi-1d-redshift1-g4k9wlebzkvp.cifk98vup2ih.us-east-1.redshift.amazonaws.com'
#datevar='"2018-02-28",'
datevar='2018-02-28'
con=psycopg2.connect(dbname= 'bidb', host=hostname, 
port= '5439', user= 'rez_haque', password= 'Password2')
cur = con.cursor()

# from datetime import date, timedelta
# d1 = date(2018,1,25)
# d2 = date(2018,1,27)

# # this will give you a list containing all of the dates
# dd = [d1 + timedelta(days=x) for x in range((d2-d1).days + 1)]

# #cur.execute("""select * from rpt.agg_domain_conversion_lvl_date_campaign_lineitem_creative where dim_ione_client_localized_report_date = %s limit 5""", (datevar,))


# LEFT OFF: couldn't get for loop to work right so try while loop? is var d below the right datatype?


# for d in dd:
#     print d
cur.execute("""
BEGIN TRANSACTION;

set search_path to rpt;

DELETE FROM
    agg_domain_conversion_lvl_date_campaign_lineitem_creative
WHERE
  dim_ione_client_localized_report_date between '2018-03-01' and '2018-04-13'
;

INSERT INTO
    agg_domain_conversion_lvl_date_campaign_lineitem_creative
(
dim_ione_client_localized_report_date
,dim_ione_business_unit_id
,dim_ione_campaign_id
,campaign_name
,campaign_flightdate_start
,campaign_flightdate_end
,campaign_account_manager_id
,campaign_status
,campaign_advertiser_source_id
,campaign_advertiser_source_name
,dim_ione_campaign_target_id
,target_name
,target_flightdate_start
,target_flightdate_end
,target_status
,dim_ione_creative_id 
,creative_name 
,dim_ione_creative_message_id 
,creative_message_name 
,creative_size 
,metric_click_based_conversions
,metric_impression_based_conversions
,metric_total_conversions
,metric_click_based_revenue
,metric_impression_based_revenue
,metric_total_revenue
)
  SELECT
    dim_ione_client_localized_report_date
    ,dim_ione_business_unit_id
    ,dim_ione_campaign_id
    ,MAX(campaign_name)
    ,MAX(campaign_flightdate_start)
    ,MAX(campaign_flightdate_end)
    ,MAX(campaign_account_manager_id)
    ,MAX(campaign_status)
    ,MAX(campaign_advertiser_source_id)
    ,MAX(campaign_advertiser_source_name)
    ,dim_ione_campaign_target_id
    ,MAX(target_name)
    ,MAX(target_flightdate_start )
    ,MAX(target_flightdate_end )
    ,MAX(target_status)
    ,dim_ione_creative_id 
    ,MAX(creative_name)
    ,MAX(dim_ione_creative_message_id)
    ,MAX(creative_message_name)
    ,MAX(creative_size)
    ,sum(NVL(metric_click_based_conversions,0)) as metric_click_based_conversions
    ,sum(NVL(metric_impression_based_conversions,0)) as metric_impression_based_conversions
    ,sum(NVL(agg.metric_click_based_conversions,0)) + SUM(NVL(agg.metric_impression_based_conversions,0)) as metric_total_conversions
    ,sum(NVL(agg.metric_click_based_revenue,0)) as metric_click_based_revenue
    ,sum(NVL(agg.metric_impression_based_revenue,0)) as metric_impression_based_revenue
    ,sum(NVL(agg.metric_click_based_revenue,0)) + SUM(NVL(agg.metric_impression_based_revenue,0)) as metric_total_revenue
FROM
        agg_domain_conversion_basedim agg
WHERE
  dim_ione_client_localized_report_date between '2018-03-01' and '2018-04-13'
GROUP BY
    dim_ione_business_unit_id
    ,dim_ione_client_localized_report_date
    ,dim_ione_campaign_id
    ,dim_ione_campaign_target_id
    ,dim_ione_creative_id
;

UPDATE agg_domain_delivery_lvl_date_campaign_lineitem_creative
SET
    conv_metric_click_based_conversions = cnv.metric_click_based_conversions
    ,conv_metric_view_based_conversions = cnv.metric_impression_based_conversions
    ,conv_metric_total_conversions = cnv.metric_total_conversions
    ,conv_metric_click_based_revenue = cnv.metric_click_based_revenue
    ,conv_metric_impression_based_revenue = cnv.metric_impression_based_revenue
    ,conv_metric_total_revenue = cnv.metric_total_revenue
FROM
    agg_domain_conversion_lvl_date_campaign_lineitem_creative cnv
WHERE
    agg_domain_delivery_lvl_date_campaign_lineitem_creative.dim_ione_business_unit_id = cnv.dim_ione_business_unit_id
    and agg_domain_delivery_lvl_date_campaign_lineitem_creative.dim_ione_client_localized_report_date =cnv.dim_ione_client_localized_report_date
    and agg_domain_delivery_lvl_date_campaign_lineitem_creative.dim_ione_campaign_id = cnv.dim_ione_campaign_id
    and agg_domain_delivery_lvl_date_campaign_lineitem_creative.dim_ione_campaign_target_id = cnv.dim_ione_campaign_target_id
    and agg_domain_delivery_lvl_date_campaign_lineitem_creative.dim_ione_creative_id = cnv.dim_ione_creative_id
    AND
     agg_domain_delivery_lvl_date_campaign_lineitem_creative.dim_ione_client_localized_report_date between '2018-03-01' and '2018-04-13'
;

commit;

END TRANSACTION;

""")

#      """, (d,))

cur.close()
con.commit()
con.close()


# COMMAND ----------

from datetime import date, timedelta
d1 = date(2018,1,26)
d2 = date(2018,1,31)

# this will give you a list containing all of the dates
dd = [d1 + timedelta(days=x) for x in range((d2-d1).days + 1)]

for d in dd:
    print d