from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from utils import *


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    sqlcontext = SQLContext(sc)

    file_json1 = '/home/lpsm/Bureau/didomi/datehour=2020-01-21-00/part-00000-3a6f210d-7806-462e-b32c-344dff538d70.c000.json'
    file_json2 = '/home/lpsm/Bureau/didomi/datehour=2020-01-21-01/part-00000-9b503de5-0d72-4099-a388-7fdcf6e16edb.c000.json'

    json_df1 = flatten_df(sqlcontext.read.json(file_json1))
    json_df2 = flatten_df(sqlcontext.read.json(file_json2))
    json_df = json_df1.union(json_df2)

    # We call one time our dataframe for optimazation
    df_pageviews = json_df.filter(json_df['type'] == 'pageview')
    df_consent_asked = json_df.filter(json_df['type'] == 'consent.asked')
    df_consent_given = json_df.filter(json_df['type'] == 'consent.given')


# metrics grouped by Date and Hour

    df_pageviews.groupBy("timestamp")\
        .count()\
        .show()

    df_pageviews.filter(json_df['user_consent'] == True)\
        .groupBy("timestamp")\
        .count()\
        .show()

    df_consent_asked.groupBy("timestamp")\
        .count()\
        .show()

    df_consent_asked.filter(json_df['user_consent'] == True)\
        .groupBy("timestamp")\
        .count()\
        .show()

    df_consent_given.groupBy("timestamp")\
        .count()\
        .show()

    df_consent_given.filter(json_df['user_consent'] == True)\
        .groupBy("timestamp")\
        .count()\
        .show()

    json_df.groupBy("timestamp")\
    	  .agg(countDistinct("user_id"))\
    	  .show()

# metric grouped by domain

    df_pageviews.groupBy("domain")\
        .count()\
        .show()

    df_pageviews.filter(json_df['user_consent'] == True)\
        .groupBy("domain")\
        .count()\
        .show()

    df_consent_asked.groupBy("domain")\
        .count()\
        .show()

    df_consent_asked.filter(json_df['user_consent'] == True)\
        .groupBy("domain")\
        .count()\
        .show()

    df_consent_given.groupBy("domain")\
        .count()\
        .show()

    df_consent_given.filter(json_df['user_consent'] == True)\
        .groupBy("domain")\
        .count()\
        .show()

    json_df.groupBy("domain")\
    	  .agg(countDistinct("user_id"))\
    	  .show()


# metric grouped by "user_countru"

    df_pageviews.groupBy("user_country")\
        .count()\
        .show()

    df_pageviews.filter(json_df['user_consent'] == True)\
        .groupBy("user_country")\
        .count()\
        .show()

    df_consent_asked.groupBy("user_country")\
        .count()\
        .show()

    df_consent_asked.filter(json_df['user_consent'] == True)\
        .groupBy("user_country")\
        .count()\
        .show()

    df_consent_given.groupBy("user_country")\
        .count()\
        .show()

    df_consent_given.filter(json_df['user_consent'] == True)\
        .groupBy("user_country")\
        .count()\
        .show()

    json_df.groupBy("user_country")\
    	  .agg(countDistinct("user_id"))\
    	  .show()
