from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession


def flatten_df(nested_df):
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)







if __name__ == '__main__':
	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext
	sqlcontext = SQLContext(sc)

	file_json1 = '/home/lpsm/Bureau/didomi/datehour=2020-01-21-00/part-00000-3a6f210d-7806-462e-b32c-344dff538d70.c000.json'
	file_json2 = '/home/lpsm/Bureau/didomi/datehour=2020-01-21-01/part-00000-9b503de5-0d72-4099-a388-7fdcf6e16edb.c000.json'

	json_df1 = flatten_df(sqlcontext.read.json(file_json1))
	json_df2 = flatten_df(sqlcontext.read.json(file_json2))
	json_df = json_df1.union(json_df2)

	df_pageviews = json_df.filter(json_df['type'] == 'pageview')
	pageviews = df_pageviews.count()
	pageviews_with_consent = df_pageviews.filter(json_df['user_consent'] == True).count()

	df_consent_asked = json_df.filter(json_df['type'] == 'consent.asked')
	consents_asked = df_consent_asked.count()
	consents_asked_with_consent = df_consent_asked.filter(json_df['user_consent'] == True).count()

	df_consent_given = json_df.filter(json_df['type'] == 'consent.given')
	consents_given = df_consent_given.count()
	consents_given_with_consent =  df_consent_given.filter(json_df['user_consent'] == True).count()
	users = json_df.select(("user_id")).distinct().count()