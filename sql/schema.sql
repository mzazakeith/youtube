-- YouTube Analytics Database Schema
-- PostgreSQL schema for storing YouTube channel and video analytics

-- Create database if it doesn't exist
-- CREATE DATABASE youtube_analytics;

-- Channel statistics table
CREATE TABLE IF NOT EXISTS channel_stats (
    id SERIAL PRIMARY KEY,
    captured_at TIMESTAMP WITH TIME ZONE NOT NULL,
    channel_id VARCHAR(50) NOT NULL,
    channel_title VARCHAR(255) NOT NULL,
    subscribers BIGINT NOT NULL,
    total_views BIGINT NOT NULL,
    video_count INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(channel_id, captured_at)
);

-- Videos table
CREATE TABLE IF NOT EXISTS videos (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(50) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    publish_time TIMESTAMP WITH TIME ZONE NOT NULL,
    views BIGINT NOT NULL DEFAULT 0,
    likes INTEGER NOT NULL DEFAULT 0,
    comments INTEGER NOT NULL DEFAULT 0,
    engagement_rate DECIMAL(10,6) NOT NULL DEFAULT 0,
    publish_day VARCHAR(20) NOT NULL,
    publish_hour INTEGER NOT NULL CHECK (publish_hour >= 0 AND publish_hour <= 23),
    likes_per_view DECIMAL(10,6) NOT NULL DEFAULT 0,
    comments_per_view DECIMAL(10,6) NOT NULL DEFAULT 0,
    views_per_day DECIMAL(15,2) NOT NULL DEFAULT 0,
    publish_date DATE NOT NULL,
    publish_day_of_week INTEGER NOT NULL CHECK (publish_day_of_week >= 1 AND publish_day_of_week <= 7),
    publish_month VARCHAR(10) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_videos_publish_time ON videos(publish_time);
CREATE INDEX IF NOT EXISTS idx_videos_views ON videos(views);
CREATE INDEX IF NOT EXISTS idx_videos_engagement_rate ON videos(engagement_rate);
CREATE INDEX IF NOT EXISTS idx_videos_publish_date ON videos(publish_date);
CREATE INDEX IF NOT EXISTS idx_videos_publish_hour ON videos(publish_hour);
CREATE INDEX IF NOT EXISTS idx_videos_publish_day_of_week ON videos(publish_day_of_week);
CREATE INDEX IF NOT EXISTS idx_channel_stats_captured_at ON channel_stats(captured_at);
CREATE INDEX IF NOT EXISTS idx_channel_stats_channel_id ON channel_stats(channel_id);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_videos_updated_at ON videos;
CREATE TRIGGER update_videos_updated_at
    BEFORE UPDATE ON videos
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create view for top performing videos
CREATE OR REPLACE VIEW top_videos AS
SELECT 
    video_id,
    title,
    publish_time,
    views,
    likes,
    comments,
    engagement_rate,
    publish_day,
    publish_hour
FROM videos 
ORDER BY views DESC
LIMIT 100;

-- Create view for daily performance summary
CREATE OR REPLACE VIEW daily_performance AS
SELECT 
    publish_date,
    COUNT(*) as video_count,
    SUM(views) as total_views,
    AVG(views) as avg_views,
    MAX(views) as max_views,
    SUM(likes) as total_likes,
    SUM(comments) as total_comments,
    AVG(engagement_rate) as avg_engagement_rate
FROM videos
GROUP BY publish_date
ORDER BY publish_date DESC;

-- Create view for hourly performance heatmap
CREATE OR REPLACE VIEW hourly_heatmap AS
SELECT 
    publish_day_of_week,
    publish_hour,
    COUNT(*) as video_count,
    AVG(views) as avg_views,
    AVG(engagement_rate) as avg_engagement_rate,
    SUM(views) as total_views
FROM videos
GROUP BY publish_day_of_week, publish_hour
ORDER BY publish_day_of_week, publish_hour;

-- Create view for monthly trends
CREATE OR REPLACE VIEW monthly_trends AS
SELECT 
    publish_month,
    COUNT(*) as video_count,
    SUM(views) as total_views,
    AVG(views) as avg_views,
    SUM(likes) as total_likes,
    SUM(comments) as total_comments,
    AVG(engagement_rate) as avg_engagement_rate
FROM videos
GROUP BY publish_month
ORDER BY publish_month;