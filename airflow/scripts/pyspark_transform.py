#!/usr/bin/env python3
"""
PySpark transformation script for YouTube analytics data.
Processes raw CSV data and computes additional metrics.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, hour, dayofweek, date_format,
    when, round as spark_round, count, sum as spark_sum,
    avg as spark_avg, max as spark_max, min as spark_min,
    current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)
from datetime import datetime

class YouTubeDataTransformer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def load_channel_data(self, file_path: str):
        """Load channel statistics data."""
        schema = StructType([
            StructField("captured_at", StringType(), True),
            StructField("channel_id", StringType(), True),
            StructField("channel_title", StringType(), True),
            StructField("subscribers", IntegerType(), True),
            StructField("total_views", IntegerType(), True),
            StructField("video_count", IntegerType(), True)
        ])
        
        df = self.spark.read.csv(file_path, header=True, schema=schema)
        
        # Convert timestamp
        df = df.withColumn("captured_at", col("captured_at").cast(TimestampType()))
        
        return df
    
    def load_videos_data(self, file_path: str):
        """Load videos data with proper schema."""
        schema = StructType([
            StructField("video_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("publish_time", StringType(), True),
            StructField("views", IntegerType(), True),
            StructField("likes", IntegerType(), True),
            StructField("comments", IntegerType(), True),
            StructField("engagement_rate", DoubleType(), True),
            StructField("publish_day", StringType(), True),
            StructField("publish_hour", IntegerType(), True)
        ])
        
        df = self.spark.read.csv(file_path, header=True, schema=schema)
        
        # Convert timestamp and add derived columns
        df = df.withColumn("publish_time", col("publish_time").cast(TimestampType()))
        df = df.withColumn("publish_date", to_date(col("publish_time")))
        df = df.withColumn("publish_day_of_week", dayofweek(col("publish_time")))
        df = df.withColumn("publish_month", date_format(col("publish_time"), "yyyy-MM"))
        
        # Additional metrics
        df = df.withColumn("likes_per_view", 
                          when(col("views") > 0, col("likes") / col("views")).otherwise(0))
        df = df.withColumn("comments_per_view", 
                          when(col("views") > 0, col("comments") / col("views")).otherwise(0))
        df = df.withColumn("views_per_day", 
                          when(col("publish_time").isNotNull(), 
                               col("views") / 
                               ((current_timestamp().cast("long") - col("publish_time").cast("long")) / (24 * 3600))
                          ).otherwise(0))
        
        return df
    
    def analyze_video_performance(self, videos_df):
        """Create video performance analytics."""
        # Top videos by views
        top_videos = videos_df.orderBy(col("views").desc()).limit(10)
        
        # Performance by day of week
        day_performance = videos_df.groupBy("publish_day", "publish_day_of_week") \
            .agg(
                count("*").alias("video_count"),
                spark_avg("views").alias("avg_views"),
                spark_avg("engagement_rate").alias("avg_engagement_rate"),
                spark_sum("views").alias("total_views")
            ) \
            .orderBy("publish_day_of_week")
        
        # Performance by hour
        hour_performance = videos_df.groupBy("publish_hour") \
            .agg(
                count("*").alias("video_count"),
                spark_avg("views").alias("avg_views"),
                spark_avg("engagement_rate").alias("avg_engagement_rate"),
                spark_sum("views").alias("total_views")
            ) \
            .orderBy("publish_hour")
        
        # Monthly trends
        monthly_trends = videos_df.groupBy("publish_month") \
            .agg(
                count("*").alias("video_count"),
                spark_avg("views").alias("avg_views"),
                spark_avg("engagement_rate").alias("avg_engagement_rate"),
                spark_sum("views").alias("total_views"),
                spark_sum("likes").alias("total_likes"),
                spark_sum("comments").alias("total_comments")
            ) \
            .orderBy("publish_month")
        
        # Engagement rate distribution
        engagement_stats = videos_df.select(
            spark_avg("engagement_rate").alias("avg_engagement"),
            spark_max("engagement_rate").alias("max_engagement"),
            spark_min("engagement_rate").alias("min_engagement"),
            spark_avg("likes_per_view").alias("avg_likes_per_view"),
            spark_avg("comments_per_view").alias("avg_comments_per_view")
        )
        
        return {
            "top_videos": top_videos,
            "day_performance": day_performance,
            "hour_performance": hour_performance,
            "monthly_trends": monthly_trends,
            "engagement_stats": engagement_stats
        }
    
    def save_processed_data(self, channel_df, videos_df, analytics, output_dir: str = "data/processed"):
        """Save processed data to Parquet files."""
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save channel data
        channel_path = f"{output_dir}/channel_stats_{timestamp}.parquet"
        channel_df.write.mode("overwrite").parquet(channel_path)
        print(f"Channel data saved to: {channel_path}")
        
        # Save videos data
        videos_path = f"{output_dir}/videos_{timestamp}.parquet"
        videos_df.write.mode("overwrite").parquet(videos_path)
        print(f"Videos data saved to: {videos_path}")
        
        # Save analytics
        for name, df in analytics.items():
            analytics_path = f"{output_dir}/{name}_{timestamp}.parquet"
            df.write.mode("overwrite").parquet(analytics_path)
            print(f"Analytics '{name}' saved to: {analytics_path}")
        
        return {
            "channel_path": channel_path,
            "videos_path": videos_path,
            "analytics_paths": {name: f"{output_dir}/{name}_{timestamp}.parquet" for name in analytics.keys()}
        }

def find_latest_file(directory: str, pattern: str):
    """Find the latest file matching the pattern."""
    if not os.path.exists(directory):
        return None
    
    files = [f for f in os.listdir(directory) if pattern in f and f.endswith('.csv')]
    if not files:
        return None
    
    # Sort by modification time
    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    return os.path.join(directory, files[0])

def main():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("YouTubeAnalyticsTransform") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print("Spark session initialized")
    
    try:
        # Find latest data files
        channel_file = find_latest_file("data/raw", "channel_stats")
        videos_file = find_latest_file("data/raw", "videos")
        
        if not channel_file:
            raise FileNotFoundError("No channel data file found in data/raw")
        if not videos_file:
            raise FileNotFoundError("No videos data file found in data/raw")
        
        print(f"Processing channel data: {channel_file}")
        print(f"Processing videos data: {videos_file}")
        
        # Initialize transformer
        transformer = YouTubeDataTransformer(spark)
        
        # Load data
        channel_df = transformer.load_channel_data(channel_file)
        videos_df = transformer.load_videos_data(videos_file)
        
        print(f"Loaded {channel_df.count()} channel records")
        print(f"Loaded {videos_df.count()} video records")
        
        # Generate analytics
        print("Generating analytics...")
        analytics = transformer.analyze_video_performance(videos_df)
        
        # Save processed data
        print("Saving processed data...")
        paths = transformer.save_processed_data(channel_df, videos_df, analytics)
        
        print("\nTransformation completed successfully!")
        print(f"Processed data saved to: data/processed")
        
        # Print some summary stats
        print("\nSummary Statistics:")
        engagement_stats = analytics["engagement_stats"].collect()[0]
        print(f"Average engagement rate: {engagement_stats['avg_engagement']:.4f}")
        print(f"Average views per video: {videos_df.agg(spark_avg('views')).collect()[0][0]:,.0f}")
        print(f"Total videos processed: {videos_df.count():,}")
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()