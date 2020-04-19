from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import col, countDistinct, SparkSession


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


	datafile_json1 = '/home/lpsm/Bureau/didomi/datehour=2020-01-21-00/part-00000-3a6f210d-7806-462e-b32c-344dff538d70.c000.json'
	datafile_json2 = '/home/lpsm/Bureau/didomi/datehour=2020-01-21-01/part-00000-9b503de5-0d72-4099-a388-7fdcf6e16edb.c000.json'

	data1 = flatten_df(sqlcontext.read.json(datafile_json1))
	data2 = flatten_df(sqlcontext.read.json(datafile_json2))
	data = data1.union(data2)

	pageviews = data.filter(data['type'] == 'pageview').count()
	pageviews_with_consent = data.filter(data['type'] == 'pageview').filter(data['user_consent'] == True).count()
	consents_asked = data.filter(data['type'] == 'consent.asked').count()
	consents_asked_with_consent = data.filter(data['type'] == 'consent.asked').filter(data['user_consent'] == True).count()
	consents_given = data.filter(data['type'] == 'consent.given').count()
	consents_given_with_consent =  data.filter(data['type'] == 'consent.given').filter(data['user_consent'] == True).count()
	users = data.agg(countDistinct(col("user_id")).alias("count")).collect()[0][0]


