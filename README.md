# YouTube Analytics Dashboard

End-to-End YouTube Channel Analytics Pipeline with PySpark, Airflow, and Grafana.

## Overview

This project implements a complete data pipeline for YouTube channel analytics:

- **Data Ingestion**: YouTube Data API v3 with pagination and quota management
- **Data Processing**: PySpark transformations and analytics
- **Orchestration**: Apache Airflow 2.7.2 for ETL pipeline management
- **Storage**: PostgreSQL database with optimized schema
- **Visualization**: Grafana dashboards for insights
- **Containerization**: Docker Compose with custom Airflow image (Java + PySpark)
- **Execution**: CeleryExecutor with Redis

## Architecture

```
YouTube API → CSV Files → PySpark → Parquet → PostgreSQL → Grafana
     ↓              ↓         ↓          ↓          ↓
   Airflow      Airflow   Airflow   Airflow   Dashboards
```

## Prerequisites

- Docker Desktop (latest version)
- Java 11+ (installed in Docker)
- Python 3.8+ (for local development - optional)
- 4GB+ RAM available for Docker

## Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd assignment-youtube
cp .env.example .env  # Create from example if exists, otherwise create manually
```

### 2. Configure Environment

Edit `.env` with your YouTube API credentials:

```bash
YOUTUBE_API_KEY=your_youtube_api_key_here
YOUTUBE_CHANNEL_ID=your_youtube_channel_id_here
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=youtube_analytics
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
```

### 3. Build and Start Services

```bash
docker-compose up -d --build
```

This will:
- Build custom Airflow image with Java 11 and PySpark
- Start all services (Airflow, PostgreSQL, Redis, Grafana, Spark)
- Wait for PostgreSQL to be healthy before starting Airflow

### 4. Initialize Airflow Database (First Time Only)

```bash
# Initialize database
docker-compose exec -T airflow-webserver airflow db init

# Create admin user
docker-compose exec -T airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### 5. Access Services

```
Airflow Dashboard:    http://localhost:8080
  Username: admin
  Password: admin

Grafana Dashboard:    http://localhost:3000
  Username: admin
  Password: admin

Spark Master UI:      http://localhost:8081

PostgreSQL:          localhost:5432
  User: airflow
  Password: airflow
  Database: youtube_analytics
```

### 6. Test the Pipeline

```bash
# Run the complete pipeline test
docker-compose exec -T airflow-worker airflow dags test youtube_analytics_pipeline 2024-01-01
```

Expected output shows all 6 tasks succeeding:
1. create_schema ✅
2. extract_youtube_data ✅
3. transform_data ✅
4. load_to_postgres ✅
5. data_quality_check ✅
6. summary_report ✅

### 7. Trigger Production Pipeline

```bash
# Trigger a manual DAG run
docker-compose exec -T airflow-scheduler airflow dags trigger youtube_analytics_pipeline

# Monitor logs
docker-compose logs -f airflow-worker
```

## Configuration

### Environment Variables (.env)

```bash
# YouTube API
YOUTUBE_API_KEY=your_api_key
YOUTUBE_CHANNEL_ID=your_channel_id

# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=youtube_analytics
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

# Airflow
AIRFLOW_UID=1000
AIRFLOW_GID=0
```

### Important: Environment Variable Changes

⚠️ If you change `.env` values after startup:

```bash
# Pull down everything and rebuild
docker-compose down
docker-compose up -d --build

# Re-initialize if needed
docker-compose exec -T airflow-webserver airflow db init
```

Simply running `docker-compose restart` will **NOT** pick up new `.env` values.

## Project Structure

```
├── airflow/
│   ├── dags/
│   │   └── youtube_analytics_dag.py    # Main Airflow DAG
│   ├── logs/                           # Airflow task logs
│   └── config/
│       └── airflow.cfg                 # Airflow configuration
│
├── scripts/
│   ├── youtube_ingest.py               # YouTube API data extraction
│   ├── pyspark_transform.py            # PySpark data transformation
│   ├── load_to_postgres.py             # PostgreSQL data loading
│   └── data_quality_check.py           # Optional: standalone quality checks
│
├── sql/
│   └── schema.sql                      # Database schema (idempotent)
│
├── data/
│   ├── raw/                            # Raw CSV files (generated)
│   └── processed/                      # Processed Parquet files (generated)
│
├── grafana/
│   ├── provisioning/
│   │   ├── dashboards/                 # Dashboard JSON definitions
│   │   └── datasources/                # Datasource configs
│   └── grafana_data/                   # Grafana persistent storage
│
├── Dockerfile                          # Custom Airflow image
├── docker-compose.yml                  # All services
├── .env                                # Environment variables
└── README.md                           # This file
```

## Services

### Airflow (Port 8080)

- **Scheduler**: Executes DAGs on schedule
- **Webserver**: UI for monitoring and management
- **Worker**: Executes tasks (CeleryExecutor)
- **Image**: Custom `youtube-analytics-airflow:latest` with Java 11 + PySpark

### PostgreSQL (Port 5435)

- **Database**: `youtube_analytics`
- **Schema**: Channel stats, videos, analytics views
- **Volume**: `postgres_data` for persistence

### Redis (Port 6379)

- **Purpose**: Message broker for Celery executor
- **Volume**: None (ephemeral)

### Grafana (Port 3000)

- **Datasource**: PostgreSQL
- **Pre-configured**: PostgreSQL connection
- **Volume**: `grafana_data` for persistence

### Spark (Port 8081)

- **Master**: Cluster coordinator
- **Worker**: Task execution
- **Purpose**: PySpark job execution

## Pipeline Tasks

### 1. Extract (`extract_youtube_data`)

**Type**: BashOperator  
**Command**: `python scripts/youtube_ingest.py`

Fetches from YouTube Data API:
- Channel statistics (subscribers, views, video count)
- Video details (title, views, likes, comments, publish time)
- Saves to `data/raw/channel_stats_*.csv` and `data/raw/videos_*.csv`

**Quota**: Uses ~102 units per run (10,000/day limit)

### 2. Transform (`transform_data`)

**Type**: BashOperator  
**Command**: `python scripts/pyspark_transform.py`

PySpark processing:
- Loads CSV files
- Calculates derived metrics (engagement rate, views per day, day of week, hour, etc.)
- Generates analytics (top videos, performance by day/hour, trends, engagement stats)
- Saves Parquet files to `data/processed/`

**Requirements**: Java 11 (pre-installed in Docker)

### 3. Load (`load_to_postgres`)

**Type**: BashOperator  
**Command**: `python scripts/load_to_postgres.py`

- Creates/updates database schema
- Upserts channel and video data (idempotent)
- Uses SQLAlchemy with PostgreSQL dialect
- Drops temporary tables after upsert

**Note**: Uses `engine.begin()` context manager (SQLAlchemy 1.4+)

### 4. Quality Check (`data_quality_check`)

**Type**: PythonOperator

Validates:
- Recent channel data exists (last 2 days)
- Recent video data exists (last 2 days)
- Raises error if validation fails

### 5. Summary Report (`summary_report`)

**Type**: PythonOperator

Generates and logs:
- Channel statistics (subscribers, views, video count)
- Video aggregates (total, average, max views, engagement rate)
- Top 5 videos by views

Output logged to Airflow UI and logs.

## Scripts

### youtube_ingest.py

```bash
# Run directly
docker-compose exec -T airflow-worker python scripts/youtube_ingest.py

# Or via Airflow
docker-compose exec -T airflow-scheduler airflow dags trigger youtube_analytics_pipeline
```

**Features**:
- Pagination for channels > 50 videos
- Quota tracking
- CSV export with timestamps
- Error handling and retry logic

### pyspark_transform.py

```bash
docker-compose exec -T airflow-worker python scripts/pyspark_transform.py
```

**Requires**: Java 11, PySpark, Python 3.8+

**Output**: 7 Parquet files
- `channel_stats_*.parquet` - Channel aggregates
- `videos_*.parquet` - Video metrics
- `top_videos_*.parquet` - Top 5 by views
- `day_performance_*.parquet` - Performance by day
- `hour_performance_*.parquet` - Performance by hour
- `monthly_trends_*.parquet` - Monthly aggregates
- `engagement_stats_*.parquet` - Engagement metrics

### load_to_postgres.py

```bash
docker-compose exec -T airflow-worker python scripts/load_to_postgres.py
```

**Database Schema**:
- `channel_stats` table - Channel data with composite key (channel_id, captured_at)
- `videos` table - Video metrics with primary key (video_id)
- Automatic timestamps and update triggers

## Testing

### Run Full Pipeline Test

```bash
docker-compose exec -T airflow-worker airflow dags test youtube_analytics_pipeline 2024-01-01
```

This runs all tasks sequentially and reports success/failure. Useful for:
- Validating data pipeline logic
- Testing in dev environments
- Checking for code errors before scheduling

### Run Individual Tasks

```bash
# Test extraction
docker-compose exec -T airflow-worker python scripts/youtube_ingest.py

# Test transformation
docker-compose exec -T airflow-worker python scripts/pyspark_transform.py

# Test loading
docker-compose exec -T airflow-worker python scripts/load_to_postgres.py
```

### View Logs

```bash
# Follow scheduler logs
docker-compose logs -f airflow-scheduler

# Follow worker logs
docker-compose logs -f airflow-worker

# View specific task logs
docker-compose exec -T airflow-worker tail -100 /opt/airflow/logs/dag_id=youtube_analytics_pipeline/run_id=*/task_id=transform_data/attempt=1.log
```

## Grafana Dashboards

Pre-built dashboards include:

- **Channel Overview**: Subscribers and views over time
- **Video Performance**: Top videos and engagement rates
- **Publishing Analytics**: Best times and days to post
- **Trend Analysis**: Monthly and daily patterns

### Add New Dashboard

1. Create Parquet query or SQL view
2. Design dashboard in Grafana UI
3. Export JSON and save to `grafana/provisioning/dashboards/`
4. Restart Grafana: `docker-compose restart grafana`

## Monitoring

### Airflow UI

- **DAG Status**: View all pipeline runs
- **Task Status**: Check individual task success/failure
- **Logs**: Real-time task output
- **Admin Panel**: Manage connections, variables, pools

### Grafana

- Real-time dashboard updates
- Time-range queries
- Custom alerts and annotations

### Docker

```bash
# View all container logs
docker-compose logs

# Follow a specific service
docker-compose logs -f airflow-worker

# View resource usage
docker stats
```

### PostgreSQL

```bash
# Connect to database
docker-compose exec postgres psql -U airflow -d youtube_analytics

# Query data
SELECT * FROM channel_stats ORDER BY captured_at DESC LIMIT 10;
SELECT COUNT(*) FROM videos;
```

## Troubleshooting

### "Airflow needs to initialize the database"

```bash
docker-compose exec -T airflow-webserver airflow db init
docker-compose restart airflow-webserver airflow-scheduler airflow-worker
```

### "No module named 'pyspark'"

Ensure Docker image rebuilt with PySpark:
```bash
docker-compose down
docker-compose up -d --build
```

### "Connection refused: postgres:5432"

PostgreSQL container not ready. Wait 30 seconds:
```bash
sleep 30
docker-compose logs postgres  # Check for errors
docker-compose exec postgres pg_isready  # Test connection
```

### "current_timestamp is not defined"

Python bytecode cache issue:
```bash
docker-compose exec -T airflow-worker find /opt/airflow/scripts -name "*.pyc" -delete
docker-compose exec -T airflow-worker find /opt/airflow/scripts -name "__pycache__" -type d -exec rm -rf {} +
docker-compose restart airflow-worker
```

### "YouTube API quota exceeded"

Check quota usage in logs. API has 10,000 units/day limit:
- Channel metadata: 1 unit
- Video statistics: 1 unit per video
- Adjust ingestion frequency in DAG schedule

### Environment variables not updating

You must restart all services:
```bash
docker-compose down
# Edit .env
docker-compose up -d --build
```

### PySpark jobs slow or failing

Check:
1. Java installation: `docker-compose exec airflow-worker java -version`
2. Spark logs: `docker-compose logs spark-master`
3. Resource usage: `docker stats`
4. Increase Docker memory allocation

## Production Deployment

Before deploying to production:

1. **Security**
   - Change all default passwords
   - Use secrets management (AWS Secrets Manager, HashiCorp Vault)
   - Enable Airflow authentication
   - Use SSL/TLS for connections

2. **Monitoring**
   - Set up alerting (PagerDuty, Slack)
   - Monitor disk space
   - Track API quota usage
   - Log centralization (ELK, CloudWatch)

3. **Scaling**
   - Increase Celery worker count
   - Add Redis Sentinel for high availability
   - Use external PostgreSQL (RDS, Azure Database)
   - Implement database replication

4. **Backups**
   - Daily PostgreSQL backups
   - Backup Grafana dashboards
   - Version control for all configs

5. **Performance**
   - Add database indexes
   - Partition large tables by date
   - Implement caching
   - Use connection pooling

## API Documentation

### YouTube Data API v3

- **Quota**: 10,000 units/day
- **Rate Limits**: 100 req/sec
- **Docs**: https://developers.google.com/youtube/v3

### PostgreSQL

- **Version**: 15
- **Port**: 5435 (mapped from 5432)
- **Driver**: psycopg2

### Airflow

- **Version**: 2.7.2
- **Executor**: CeleryExecutor
- **Broker**: Redis

### PySpark

- **Version**: Latest (3.x)
- **Java**: OpenJDK 11
- **Deployment**: Local[*]

## Contributing

1. Create a feature branch
2. Test changes: `docker-compose exec -T airflow-worker airflow dags test youtube_analytics_pipeline 2024-01-01`
3. Update this README if needed
4. Submit a pull request

## License

Educational purposes only.

## Support

For issues:
1. Check logs: `docker-compose logs [service]`
2. Review Troubleshooting section above
3. Test components individually
4. Check YouTube API quota
5. Verify environment variables

---

**Last Updated**: November 2025  
**Pipeline Status**: ✅ All tasks passing
