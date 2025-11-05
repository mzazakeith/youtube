#!/usr/bin/env python3
"""
YouTube Data API v3 ingestion script with pagination support.
Fetches channel statistics and video data with quota-aware scheduling.
"""

import os
import requests
import json
import pandas as pd
from datetime import datetime, timezone
import time
from typing import Dict, List, Optional

class YouTubeDataIngestor:
    def __init__(self, api_key: str, channel_id: str):
        self.api_key = api_key
        self.channel_id = channel_id
        self.base_url = "https://www.googleapis.com/youtube/v3"
        self.quota_used = 0
        self.max_quota = 10000  # Daily quota limit
        
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """Make API request with error handling and quota tracking."""
        params['key'] = self.api_key
        
        try:
            response = requests.get(f"{self.base_url}/{endpoint}", params=params)
            response.raise_for_status()
            
            # Track quota usage (rough estimation)
            if endpoint == 'search':
                self.quota_used += 100
            elif endpoint == 'videos':
                self.quota_used += 1
            elif endpoint == 'channels':
                self.quota_used += 1
                
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            raise
    
    def get_channel_info(self) -> Dict:
        """Get channel statistics."""
        params = {
            'part': 'snippet,statistics',
            'id': self.channel_id
        }
        
        response = self._make_request('channels', params)
        
        if not response.get('items'):
            raise ValueError(f"Channel {self.channel_id} not found")
            
        channel_data = response['items'][0]
        
        return {
            'captured_at': datetime.now(timezone.utc).isoformat(),
            'channel_id': self.channel_id,
            'channel_title': channel_data['snippet']['title'],
            'subscribers': int(channel_data['statistics'].get('subscriberCount', 0)),
            'total_views': int(channel_data['statistics'].get('viewCount', 0)),
            'video_count': int(channel_data['statistics'].get('videoCount', 0))
        }
    
    def get_all_videos(self, max_results: int = 50) -> List[Dict]:
        """Get all videos from the channel with pagination."""
        videos = []
        next_page_token = None
        
        while True:
            params = {
                'part': 'snippet',
                'channelId': self.channel_id,
                'maxResults': min(max_results, 50),
                'order': 'date',
                'type': 'video'
            }
            
            if next_page_token:
                params['pageToken'] = next_page_token
            
            response = self._make_request('search', params)
            
            for item in response.get('items', []):
                video_id = item['id']['videoId']
                video_details = self.get_video_details(video_id)
                if video_details:
                    videos.append(video_details)
            
            next_page_token = response.get('nextPageToken')
            
            if not next_page_token or len(videos) >= max_results:
                break
                
            # Rate limiting to avoid quota exhaustion
            time.sleep(0.1)
        
        return videos
    
    def get_video_details(self, video_id: str) -> Optional[Dict]:
        """Get detailed statistics for a single video."""
        params = {
            'part': 'snippet,statistics',
            'id': video_id
        }
        
        try:
            response = self._make_request('videos', params)
            
            if not response.get('items'):
                return None
                
            video_data = response['items'][0]
            snippet = video_data['snippet']
            stats = video_data['statistics']
            
            # Parse publish time
            publish_time = datetime.fromisoformat(
                snippet['publishedAt'].replace('Z', '+00:00')
            )
            
            # Calculate engagement rate
            views = int(stats.get('viewCount', 0))
            likes = int(stats.get('likeCount', 0))
            comments = int(stats.get('commentCount', 0))
            engagement_rate = (likes + comments) / views if views > 0 else 0
            
            return {
                'video_id': video_id,
                'title': snippet['title'],
                'publish_time': publish_time.isoformat(),
                'views': views,
                'likes': likes,
                'comments': comments,
                'engagement_rate': engagement_rate,
                'publish_day': publish_time.strftime('%A'),
                'publish_hour': publish_time.hour
            }
            
        except Exception as e:
            print(f"Error fetching details for video {video_id}: {e}")
            return None
    
    def save_data(self, channel_data: Dict, videos_data: List[Dict], output_dir: str = 'data/raw'):
        """Save data to CSV files."""
        os.makedirs(output_dir, exist_ok=True)
        
        # Save channel data
        channel_df = pd.DataFrame([channel_data])
        channel_file = os.path.join(output_dir, f"channel_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        channel_df.to_csv(channel_file, index=False)
        print(f"Channel data saved to: {channel_file}")
        
        # Save videos data
        if videos_data:
            videos_df = pd.DataFrame(videos_data)
            videos_file = os.path.join(output_dir, f"videos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            videos_df.to_csv(videos_file, index=False)
            print(f"Videos data saved to: {videos_file}")
        
        return channel_file, videos_file if videos_data else None

def main():
    # Configuration
    API_KEY = os.getenv('YOUTUBE_API_KEY')
    CHANNEL_ID = os.getenv('YOUTUBE_CHANNEL_ID', 'UCE4eioDmSOwZi3sW3-QeiaA')
    
    if not API_KEY:
        raise ValueError("YOUTUBE_API_KEY environment variable must be set")
    
    print(f"Starting YouTube data ingestion for channel: {CHANNEL_ID}")
    print(f"API Key: {API_KEY[:10]}...")
    
    # Initialize ingestor
    ingestor = YouTubeDataIngestor(API_KEY, CHANNEL_ID)
    
    try:
        # Get channel information
        print("Fetching channel information...")
        channel_data = ingestor.get_channel_info()
        print(f"Channel: {channel_data['channel_title']}")
        print(f"Subscribers: {channel_data['subscribers']:,}")
        print(f"Total Views: {channel_data['total_views']:,}")
        print(f"Video Count: {channel_data['video_count']:,}")
        
        # Get videos data
        print("\nFetching videos data...")
        videos_data = ingestor.get_all_videos(max_results=100)  # Limit for demo
        print(f"Fetched {len(videos_data)} videos")
        print(f"Quota used: {ingestor.quota_used}")
        
        # Save data
        print("\nSaving data...")
        channel_file, videos_file = ingestor.save_data(channel_data, videos_data)
        
        print(f"\nIngestion completed successfully!")
        print(f"Total quota used: {ingestor.quota_used} out of {ingestor.max_quota}")
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise

if __name__ == "__main__":
    main()