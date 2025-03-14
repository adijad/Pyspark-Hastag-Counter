from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, explode, col, lower, regexp_extract, split, trim

def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def read_tweets_data(spark, input_path):
    return spark.read.json(input_path)

def extract_hashtags(df, column_name):
    cleaned_texts = regexp_replace(df[column_name], r'[^#\w\s]', ' ')
    words_df = df.select(explode(split(cleaned_texts, r'\s+')).alias('word'))
    hashtags_df = words_df.filter(col("word").rlike(r"#[A-Za-z]\w*")) \
                           .select(trim(lower(col("word"))).alias("hashtag"))
    return hashtags_df

def combine_hashtags(hashtags_dfs):
    combined_df = hashtags_dfs[0]
    for df in hashtags_dfs[1:]:
        combined_df = combined_df.union(df)
    return combined_df

def count_hashtags(combined_df):
    return combined_df.groupBy("hashtag").count()

def write_top_hashtags(top_hashtags, output_path):
    top_hashtags.write.csv(output_path, header=True)

def main():
    input_path = "s3://p2dataadityajadhav/smallTwitter-1.json"
    output_top_path = "s3://p2dataadityajadhav/output/top_hashtag_counts.csv"

    spark = create_spark_session("Twitter Hashtag Analysis")
    tweets_df = read_tweets_data(spark, input_path)

    hashtags_doc_text = extract_hashtags(tweets_df, "doc.text")
    hashtags_user_description = extract_hashtags(tweets_df, "doc.user.description")
    hashtags_value_properties_text = extract_hashtags(tweets_df, "value.properties.text")

    combined_hashtags_df = combine_hashtags([hashtags_doc_text, hashtags_user_description, hashtags_value_properties_text])
    hashtag_counts = count_hashtags(combined_hashtags_df)
    top_hashtags = hashtag_counts.orderBy(col("count").desc()).limit(20)

    write_top_hashtags(top_hashtags, output_top_path)
    top_hashtags.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
