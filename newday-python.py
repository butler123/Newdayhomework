from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Spark-Session-Newday").getOrCreate()

# Read files into DataFrame
ratings_file_path = "./data/ratings.dat"
movies_file_path = "./data/movies.dat"
users_file_path = "./data/users.dat"
ratings = spark.read.csv(ratings_file_path, sep='::', schema='UserID int, MovieID int, Rating int, Timestamp long')
movies = spark.read.csv(movies_file_path, sep='::', schema='MovieID int, Title string, Genres string')

# Register the DataFrames as temporary tables
ratings.createOrReplaceTempView("ratings")
movies.createOrReplaceTempView("movies")

#Create the ratings aggregations table
ratings_aggregations = spark.sql("select movies.MovieID, movies.Title, movies.Genres, max(ratings.rating) as MaxRating, min(ratings.rating) as MinRating, round(avg(ratings.rating),2) as AverageRating "
                        "from ratings join movies on movies.MovieID = ratings.MovieID group by movies.MovieID, Title, Genres")

#Create the users top 3 movies table
users_top_3 = spark.sql("with cte1 as (select UserID, MovieID, rating, row_number() over (partition by UserID order by rating desc) as rn from ratings), "
                        "cte2 as (select cte1.*, Title from cte1 join Movies on cte1.MovieId = Movies.MovieID where rn < 4) "
                        "select UserID, concat_ws(', ', collect_set(Title)) as Top3Movies from cte2 group by UserID")

##Show the dataframes
print("Ratings Table: ")
ratings.show()

print("Movies Table: ")
movies.show()

print("Aggregated Ratings Table: ")
ratings_aggregations.show()

print("Users top 3 movies table: ")
users_top_3.show()

spark.stop()