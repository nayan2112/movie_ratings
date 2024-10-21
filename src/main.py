from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, explode, split, regexp_extract, broadcast

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Movie Ratings Analysis") \
    .config("spark.executor.memory", "100G") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "40") \
    .config("spark.driver.memory", "32G") \
    .config("spark.sql.shuffle.partitions", "160") \
    .config("spark.local.dir", "/path/to/ssd") \
    .config("spark.network.timeout", "800s") \
    .master("local[*]") \
    .getOrCreate()


file_path_movies = "data/movies.dat"
file_path_users = "data/users.dat"
file_path_ratings = "data/ratings.dat"

# Reading movies file
movies_df = spark.read.csv(file_path_movies, sep='::', header=False, inferSchema=True)
movies_df = movies_df.toDF("MovieID", "Title", "Genres")
# Reading users file and selecting required columns
users_df = spark.read.csv(file_path_users, sep='::', header=False, inferSchema=True)
users_df = users_df.toDF("UserID", "Gender", "Age", "Occupation", "Zip-code").select("UserID", "Age")
# Reading ratings file and selecting required columns
ratings_df = spark.read.csv(file_path_ratings, sep='::', header=False, inferSchema=True)
ratings_df = ratings_df.toDF("UserID", "MovieID", "Rating", "Timestamp").select("UserID", "MovieID", "Rating")


# Extract the year from the movie title
movies_df = movies_df.withColumn("Year", regexp_extract(col("title"), r"\((\d{4})\)", 1).cast("int"))
# Filter movies released after 1989
movies_df_filtered = movies_df.filter(col("Year") > 1989)
# Split the genres into an array
movies_df_filtered = movies_df_filtered.withColumn("Genre", explode(split(col("Genres"), "\|"))).select("MovieId", "Genre", "year")
movies_df_filtered.cache()

#Filter the user aged 18-49 years  
users_df_filtered = users_df.filter((col("Age") >= 18) & (col("Age") <= 49))
users_df_filtered.cache()

#Filter rating table based on user aged 18-49 years
ratings_df_filtered = ratings_df.join(broadcast(users_df_filtered), "UserID").select("UserID", "MovieID", "Rating")

#Filter rating table again based on movie released after 1989
movies_ratings_joined = ratings_df_filtered.join(broadcast(movies_df_filtered), "movieId")

#Unpersisting tables
movies_df_filtered.unpersist()
users_df_filtered.unpersist()

# Repartitioning the DataFrame by the 'Genre' column
movies_ratings_repartitioned = movies_ratings_joined.repartition("Genre")


# Group by genre and year, and calculate the average rating
avg_ratings = movies_ratings_joined.groupBy("Genre", "Year").agg(avg("Rating").alias("Avg_Rating"))
avg_ratings.show()

spark.stop()















