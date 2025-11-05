#!/usr/bin/env python3
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgresLoader:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
        
    def execute_schema(self, schema_file: str = 'sql/schema.sql'):
        try:
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            with self.engine.begin() as conn:
                conn.execute(text(schema_sql))
            logger.info('Database schema created/updated successfully')
        except Exception as e:
            logger.error(f'Error executing schema: {e}')
            raise
    
    def upsert_channel_stats(self, df: pd.DataFrame):
        if df.empty:
            logger.warning('No channel stats data to upsert')
            return
        try:
            with self.engine.begin() as conn:
                temp_table = 'temp_channel_stats'
                df.to_sql(temp_table, conn, if_exists='replace', index=False)
                upsert_query = f'''
                INSERT INTO channel_stats (captured_at, channel_id, channel_title, subscribers, total_views, video_count)
                SELECT captured_at, channel_id, channel_title, subscribers, total_views, video_count
                FROM {temp_table}
                ON CONFLICT (channel_id, captured_at) 
                DO UPDATE SET
                    channel_title = EXCLUDED.channel_title,
                    subscribers = EXCLUDED.subscribers,
                    total_views = EXCLUDED.total_views,
                    video_count = EXCLUDED.video_count,
                    created_at = CURRENT_TIMESTAMP
                '''
                conn.execute(text(upsert_query))
                conn.execute(text(f'DROP TABLE IF EXISTS {temp_table}'))
            logger.info(f'Upserted {len(df)} channel stats records')
        except Exception as e:
            logger.error(f'Error upserting channel stats: {e}')
            raise
    
    def upsert_videos(self, df: pd.DataFrame):
        if df.empty:
            logger.warning('No videos data to upsert')
            return
        try:
            with self.engine.begin() as conn:
                temp_table = 'temp_videos'
                df.to_sql(temp_table, conn, if_exists='replace', index=False)
                upsert_query = f'''
                INSERT INTO videos (
                    video_id, title, publish_time, views, likes, comments, 
                    engagement_rate, publish_day, publish_hour, likes_per_view,
                    comments_per_view, views_per_day, publish_date, 
                    publish_day_of_week, publish_month
                )
                SELECT 
                    video_id, title, publish_time, views, likes, comments, 
                    engagement_rate, publish_day, publish_hour, likes_per_view,
                    comments_per_view, views_per_day, publish_date, 
                    publish_day_of_week, publish_month
                FROM {temp_table}
                ON CONFLICT (video_id) 
                DO UPDATE SET title = EXCLUDED.title, views = EXCLUDED.views, updated_at = CURRENT_TIMESTAMP
                '''
                conn.execute(text(upsert_query))
                conn.execute(text(f'DROP TABLE IF EXISTS {temp_table}'))
            logger.info(f'Upserted {len(df)} video records')
        except Exception as e:
            logger.error(f'Error upserting videos: {e}')
            raise
    
    def load_from_parquet(self, channel_path: str, videos_path: str):
        try:
            if os.path.exists(channel_path):
                channel_df = pd.read_parquet(channel_path)
                self.upsert_channel_stats(channel_df)
            if os.path.exists(videos_path):
                videos_df = pd.read_parquet(videos_path)
                self.upsert_videos(videos_df)
        except Exception as e:
            logger.error(f'Error loading from Parquet: {e}')
            raise

def find_latest_parquet(directory: str, pattern: str):
    if not os.path.exists(directory):
        return None
    files = [f for f in os.listdir(directory) if pattern in f and f.endswith('.parquet')]
    if not files:
        return None
    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    return os.path.join(directory, files[0])

def main():
    db_host = os.getenv('POSTGRES_HOST', 'localhost')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    db_name = os.getenv('POSTGRES_DB', 'youtube_analytics')
    db_user = os.getenv('POSTGRES_USER', 'airflow')
    db_password = os.getenv('POSTGRES_PASSWORD', 'airflow')
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    logger.info('Starting PostgreSQL data load')
    try:
        loader = PostgresLoader(connection_string)
        logger.info('Creating/updating database schema...')
        loader.execute_schema()
        channel_file = find_latest_parquet('data/processed', 'channel_stats')
        videos_file = find_latest_parquet('data/processed', 'videos')
        if not channel_file and not videos_file:
            logger.error('No processed data files found')
            return
        logger.info('Loading data into PostgreSQL...')
        loader.load_from_parquet(channel_file, videos_file)
        logger.info('Load completed successfully!')
    except Exception as e:
        logger.error(f'Error during data load: {e}')
        raise

if __name__ == '__main__':
    main()
