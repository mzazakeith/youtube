"""
YouTube Analytics Pipeline DAG
Extract - Transform - Load pipeline for YouTube channel analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import os

# Default arguments for the DAG
default_args = {
    'owner': 'youtube-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Create DAG
dag = DAG(
    'youtube_analytics_pipeline',
    default_args=default_args,
    description='YouTube Analytics ETL Pipeline',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['youtube', 'analytics', 'etl'],
)

# Environment variables
def get_env_vars():
    """Get environment variables for the pipeline."""
    return {
        'YOUTUBE_API_KEY': Variable.get('YOUTUBE_API_KEY', default_var='AIzaSyAKWJ_Z2F8ORTgT36_t6yIZaUpYVg3by28'),
        'YOUTUBE_CHANNEL_ID': Variable.get('YOUTUBE_CHANNEL_ID', default_var='UCWpxNm3hgndKBhwnnrVsFkA'),
        'POSTGRES_HOST': Variable.get('POSTGRES_HOST', default_var='postgres'),
        'POSTGRES_PORT': Variable.get('POSTGRES_PORT', default_var='5432'),
        'POSTGRES_DB': Variable.get('POSTGRES_DB', default_var='youtube_analytics'),
        'POSTGRES_USER': Variable.get('POSTGRES_USER', default_var='airflow'),
        'POSTGRES_PASSWORD': Variable.get('POSTGRES_PASSWORD', default_var='airflow'),
        'PYTHONPATH': '/opt/airflow',
    }

# Task 1: Extract data from YouTube API
extract_youtube_data = BashOperator(
    task_id='extract_youtube_data',
    bash_command='cd /opt/airflow && python scripts/youtube_ingest.py',
    env=get_env_vars(),
    dag=dag,
)

# Task 2: Transform data with PySpark
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='cd /opt/airflow && python scripts/pyspark_transform.py',
    env=get_env_vars(),
    dag=dag,
)

# Task 3: Load data to PostgreSQL
load_to_postgres = BashOperator(
    task_id='load_to_postgres',
    bash_command='cd /opt/airflow && python scripts/load_to_postgres.py',
    env=get_env_vars(),
    dag=dag,
)

# Task 4: Create database schema (idempotent)
# Read SQL file directly to avoid template path issues
with open('/opt/airflow/sql/schema.sql', 'r') as f:
    schema_sql = f.read()

create_schema = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id='postgres_default',
    sql=schema_sql,
    dag=dag,
)

# Task 5: Data quality check
def check_data_quality(**context):
    """Check data quality after loading."""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check if we have recent data
    channel_check = postgres_hook.get_first("""
        SELECT COUNT(*) FROM channel_stats 
        WHERE captured_at >= NOW() - INTERVAL '2 days'
    """)
    
    videos_check = postgres_hook.get_first("""
        SELECT COUNT(*) FROM videos 
        WHERE created_at >= NOW() - INTERVAL '2 days'
    """)
    
    if channel_check[0] == 0:
        raise ValueError("No recent channel data found")
    
    if videos_check[0] == 0:
        raise ValueError("No recent video data found")
    
    print(f"Data quality check passed: {channel_check[0]} channel records, {videos_check[0]} video records")

data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,
    dag=dag,
)

# Task 6: Generate summary report
def generate_summary_report(**context):
    """Generate and log a summary report."""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Channel summary
    channel_summary = postgres_hook.get_first("""
        SELECT channel_title, subscribers, total_views, video_count, captured_at
        FROM channel_stats 
        ORDER BY captured_at DESC 
        LIMIT 1
    """)
    
    # Video summary
    video_summary = postgres_hook.get_first("""
        SELECT 
            COUNT(*) as total_videos,
            SUM(views) as total_views,
            AVG(views) as avg_views,
            MAX(views) as max_views,
            AVG(engagement_rate) as avg_engagement_rate
        FROM videos
    """)
    
    # Top videos
    top_videos = postgres_hook.get_records("""
        SELECT title, views, engagement_rate, publish_time
        FROM videos 
        ORDER BY views DESC 
        LIMIT 5
    """)
    
    print("=" * 60)
    print("YOUTUBE ANALYTICS SUMMARY REPORT")
    print("=" * 60)
    
    if channel_summary:
        print(f"Channel: {channel_summary[0]}")
        print(f"Subscribers: {channel_summary[1]:,}")
        print(f"Total Views: {channel_summary[2]:,}")
        print(f"Video Count: {channel_summary[3]:,}")
        print(f"Last Updated: {channel_summary[4]}")
    
    print("\nVideo Statistics:")
    if video_summary:
        print(f"Total Videos: {video_summary[0]:,}")
        print(f"Total Views: {video_summary[1]:,}")
        print(f"Average Views: {video_summary[2]:,.0f}")
        print(f"Max Views: {video_summary[3]:,}")
        print(f"Average Engagement Rate: {video_summary[4]:.4f}")
    
    print("\nTop 5 Videos:")
    for i, video in enumerate(top_videos, 1):
        print(f"{i}. {video[0][:50]}... - {video[1]:,} views (ER: {video[2]:.4f})")
    
    print("=" * 60)

summary_report = PythonOperator(
    task_id='summary_report',
    python_callable=generate_summary_report,
    dag=dag,
)

# Define task dependencies
create_schema >> extract_youtube_data >> transform_data >> load_to_postgres >> data_quality_check >> summary_report

# Add documentation
dag.doc_md = """
### YouTube Analytics Pipeline

This DAG implements a complete ETL pipeline for YouTube channel analytics:

**Tasks:**
1. **create_schema**: Creates/updates PostgreSQL database schema
2. **extract_youtube_data**: Fetches data from YouTube API using pagination
3. **transform_data**: Processes data with PySpark, computes metrics
4. **load_to_postgres**: Loads processed data to PostgreSQL with upserts
5. **data_quality_check**: Validates data quality
6. **summary_report**: Generates and logs summary statistics

**Dependencies:**
- Requires uv to be installed in the Airflow container
- Requires PostgreSQL connection configured as 'postgres_default'
- Requires YouTube API key and channel ID as Airflow Variables

**Schedule:**
- Runs daily at midnight
- Can be triggered manually
- Supports backfilling

**Data Flow:**
YouTube API → CSV files → Parquet files → PostgreSQL → Grafana dashboards
"""