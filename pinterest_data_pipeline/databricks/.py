dbutils.fs.ls("/FileStore/tables")
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib
# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")
# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")
# AWS S3 bucket name
AWS_S3_BUCKET = "user-0e0816526d11-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/user-0e0816526d11-bucket"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
# list the topics stored on the mounted S3 bucket
display(dbutils.fs.ls("/mnt/user-0e0816526d11-bucket/topics"))
# create path to topic files
file_location = "/mnt/user-0e0816526d11-bucket/topics/0e0816526d11.pin/partition=0/*.json"
# specify file type
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# load JSONs from mounted S3 bucket to Spark dataframe
df_pin = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(file_location)

# cleaning Pinterest dataframe
df_pin = df_pin.replace("No description available Story format", None)
df_pin = df_pin.replace("null", None)
df_pin = df_pin.replace("User Info Error", None)
df_pin = df_pin.replace("Image src error", None)
df_pin = df_pin.replace("N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", None)
df_pin = df_pin.replace("No Title Data Available", None)

# transforms the 'follower_count' col from string to integer and checks if the value matches a pattern that contains either k or M 
df_pin = df_pin.withColumn("follower_count", when(
    col("follower_count").rlike("\d+k"),(regexp_extract(col("follower_count"),"(\d+)",1).cast("integer") * 1000)).when(col("follower_count").rlike("\d+M"),(regexp_extract(col("follower_count"), "(\d+)", 1).cast("integer") * 1000000))
# otherwise, if it doesn't matches it leaves the full integer value
.otherwise(col("follower_count").cast("integer")))

# cleaning the 'save_location' column by removing 'Local save in ' text and just leaving the path for the 'save_location' column
df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# Renaming the column
df_pin = df_pin.withColumnRenamed("index", "ind")

df_pin = df_pin.dropDuplicates(["unique_id"])
df_pin = df_pin.na.drop()

# rearranging the Pinterest columns
reorder_col = ["ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category"]
df_pin = df_pin.select(reorder_col)

# Show the table of the Pinterest data
# display(df_pin)
file_location = "/mnt/user-0e0816526d11-bucket/topics/0e0816526d11.geo/partition=0/*.json"
# specify file type
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# load JSONs from mounted S3 bucket to Spark dataframe
df_geo = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(file_location)

# created a new column containing latitude and longitude
df_geo = df_geo.withColumn("coordinates", array(col("latitude"), col("longitude")))

# dropping columns
df_geo = df_geo.drop("latitude", "longitude")
df_geo = df_geo.withColumn("timestamp", col("timestamp").cast("timestamp"))
df_geo = df_geo.dropDuplicates(["ind","country", "coordinates", "timestamp"])
# df_geo = df_geo.na.drop()

geo_reorder_col = ["ind", "country", "coordinates", "timestamp"]
df_geo = df_geo.select(geo_reorder_col)

df_geo = df_geo.sort("country")

# Show the table of the Geolocation data
# display(df_geo)
file_location = "/mnt/user-0e0816526d11-bucket/topics/0e0816526d11.user/partition=0/*.json"
# specify file type
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# load JSONs from mounted S3 bucket to Spark dataframe
df_user = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(file_location)

df_user = df_user.withColumn("user_name", concat(col("first_name"),lit(" "),col("last_name")))
df_user = df_user.drop("first_name","last_name","index")
df_user = df_user.withColumn("date_joined", col("date_joined").cast("timestamp"))
df_user = df_user.dropDuplicates(["user_name", "age", "date_joined"])
df_user = df_user.na.drop()

user_reorder_col = ["ind","user_name","age","date_joined"]
df_user = df_user.select(user_reorder_col)

df_user = df_user.sort("user_name")

# Show the table of the User data
# display(df_user)
dbutils.fs.unmount("/mnt/user-0e0816526d11-bucket")
# Finding the most popular Pinterest category in each country.

df_pin_geo = df_pin.join(df_geo, "ind").groupBy(df_geo.country, df_pin.category).agg(count("category").alias("category_count")).orderBy(desc("category_count"))

display(df_pin_geo)

# Finding the most popular category each year

df_post_year = df_pin.join(df_geo, "ind").groupBy(year("timestamp").alias("post_year"), "category").agg(count("category").alias("category_count"))

post_year_filter = df_post_year.filter((col("post_year") >= 2018) & (col("post_year") <= 2022))

popular_cat_each_year = post_year_filter.groupBy("post_year").agg(first("category").alias("category"), first("category_count").alias("category_count")).orderBy(desc("post_year"))

display(popular_cat_each_year)
# Finding user with most follower in each country

df_c_pn_fc = df_pin.join(df_geo, "ind").groupBy(df_geo.country, df_pin.poster_name).agg(first(df_pin.follower_count).alias("follower_count")).orderBy(desc("follower_count"))
display(df_c_pn_fc)

# country with most user followers

df_c_fc = df_c_pn_fc.orderBy(desc("follower_count"))
df_c_fc = df_c_fc.drop("poster_name")
df_c_fc.show(1)
# Finding the most popular category for different age groups

# age groups:
# 18 <-> 24
# 25 <-> 35
# 36 <-> 50
# +50

df_age_group = df_user.withColumn("age_group", when((df_user.age >=18) & (df_user.age <= 24), "18 - 24").when((df_user.age >=25) & (df_user.age <= 35), "25 - 35").when((df_user.age >=36) & (df_user.age <= 50), "36 - 50").otherwise("Invalid"))

df_pin_age_group_join = df_pin.join(df_age_group, "ind").groupBy(df_age_group.age_group, df_pin.category).agg(count("*").alias("category_count")).orderBy(desc("category_count"))

popular_cat_by_age_group = df_pin_age_group_join.groupBy("age_group").agg(first("category").alias("category"), first("category_count").alias("category_count"))

display(popular_cat_by_age_group)
# Finding the median follower count for different age groups

df_median = df_pin.join(df_age_group, "ind").groupBy(df_age_group.age_group, df_pin.follower_count).agg(count("*").alias("median_follower_count"))

df_median_age_group = df_median.groupBy("age_group").agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count")).orderBy(asc("age_group"))

display(df_median_age_group)

# Finding how many users have joined each year?

df_user_post_year = df_user.withColumn("post_year", year("date_joined"))

df_user_joined = df_user_post_year.groupBy("post_year").agg(count("*").alias("number_users_joined"))

user_post_year_filter = df_user_joined.filter((col("post_year") >= 2015) & (col("post_year") <= 2020))

user_joined_each_year = user_post_year_filter.groupBy("post_year").agg(first("number_users_joined").alias("number_users_joined")).orderBy(desc("post_year"))

display(user_joined_each_year)
# Finding the median follower count of users that have joined between 2015 to 2020

df_pin_user_median_follower_count = df_pin.join(df_user_post_year, "ind").groupBy(df_user_post_year.post_year, df_pin.follower_count).agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))

df_user_post_year_filter = df_pin_user_median_follower_count.filter((col("post_year") >= 2015) & (col("post_year") <= 2020))

user_mfc = df_user_post_year_filter.groupBy("post_year").agg(first("median_follower_count").alias("median_follower_count")).orderBy(desc("post_year"))

display(user_mfc)
# Finding the median follower count of users based on their joining and age group

df_pin_age_group_join2 = df_pin.join(df_age_group, "ind").groupBy(df_age_group.age_group, df_pin.follower_count).agg(count("*").alias("median_follower_count")).orderBy(desc("median_follower_count"))

df_pin_user_median_follower_count = df_pin.join(df_user_post_year, "ind").groupBy(df_user_post_year.post_year, df_pin.follower_count).agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count"))

df_age_group_post_year2 = df_pin_age_group_join2.join(df_pin_user_median_follower_count, "follower_count").groupBy(df_pin_age_group_join2.age_group, df_pin_user_median_follower_count.post_year).agg(expr("percentile_approx(follower_count, 0.5)").alias("median_follower_count")).orderBy(asc("age_group"))

# filters the post year between 2015 to 2020
df_age_group_post_year_filter = df_age_group_post_year2.filter((col("post_year") >= 2015) & (col("post_year") <= 2020))

display(df_age_group_post_year_filter)