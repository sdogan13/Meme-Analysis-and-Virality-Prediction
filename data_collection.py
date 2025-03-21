import os
import sys
import time
import json
import gzip
import psutil
import sqlite3
import logging
import random
import praw
import pandas as pd
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler

# Load environment variables
load_dotenv()

# File paths
DATA_PATH = os.path.expanduser('~/Reddit_Virality_Data')
LOG_PATH = f"{DATA_PATH}/logs"
os.makedirs(DATA_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

# Configure logging - REDUCED VERBOSITY
logger = logging.getLogger('reddit_tracker')
logger.setLevel(logging.INFO)

# Console handler - MORE CONCISE FORMATTING
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# File handler
log_file = f"{LOG_PATH}/reddit_tracker_{datetime.now().strftime('%Y%m%d')}.log"
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

CATEGORIES = ['new', 'hot', 'rising', 'top']
SUBREDDIT_GROUPS = {
    # English language meme subreddits - verified and meme-focused
    'english_memes': [
        # Major general meme subreddits
        'memes', 'dankmemes', 'me_irl', 'wholesomememes',
        'PrequelMemes', 'SequelMemes', 'OTMemes', 'MemeEconomy', 'wholesomemes',
        
        # Topic-specific meme subreddits
        'historymemes', 'ProgrammerHumor', 'lotrmemes', 'formuladank',
        'surrealmemes', 'antimeme', 'bonehurtingjuice', 'shitposting',
        'bikinibottomtwitter', 'raimimemes'
    ],
    
    # Non-English language memes - verified meme-focused
    'international_memes': [
        # French meme subreddits
        'rance', 'moi_dlvv', 'FrenchMemes',
        
        # Spanish/Latin meme subreddits
        'yo_elvr', 'SpanishMeme', 'dankgentina', 'memesmexico', 
        
        # German meme subreddits
        'ich_iel', 'okbrudimongo', 'deutschememes',
        
        # Italian meme subreddits
        'memesITA',
        
        # Nordic meme subreddits
        'unket', 'dankmark',
        
        # Turkish meme subreddits
        'TurkeyJerky', 'burdurland',
        
        # Portuguese/Brazilian meme subreddits
        'eu_nvr', 'DiretoDoZapZap'
    ]
}

# API rotation and throttling
API_KEYS_COUNT = 4
RATE_LIMIT_BUFFER = 100
RATE_LIMIT_PAUSE = 60

# Tracking intervals based on post age (in hours)
AGE_TRACKING_INTERVALS = {
    1: 5,     # 0-1 hours: check every 5 minutes
    3: 10,    # 1-3 hours: check every 10 minutes
    6: 15,    # 3-6 hours: check every 15 minutes
    12: 30,   # 6-12 hours: check every 30 minutes
    24: 60,   # 12-24 hours: check every 60 minutes
    48: 120,  # 24-48 hours: check every 120 minutes
    72: 240,  # 48-72 hours: check every 240 minutes
    168: 360,  # 72-168 hours: check every 480 minutes
    float('inf'): 720  # >168 hours: check every 480 minutes
}

# Archiving settings
ARCHIVE_AFTER_DAYS = 30
BACKUP_DAYS = 7

# Maximum number of authentication failures before skipping a subreddit
MAX_AUTH_FAILURES = 5

def current_time():
    """Get current UTC timestamp"""
    return int(time.time())

class RedditTracker:
    """Track Reddit posts over time to analyze virality patterns"""
    
    def __init__(self):
        """Initialize the tracker"""
        # Set up database
        self.db_lock = threading.Lock()
        self.conn = self._init_database()
        
        # Set up API clients
        self.reddit_clients = []
        self.current_client_idx = 0
        self.api_calls_count = 0
        self._setup_reddit_clients()
        
        # Set up tracking state
        self.state = {}
        self.state_file = f"{DATA_PATH}/tracker_state.txt"
        self._load_state()
        
        # Caches
        self.subreddit_cache = {}
        self.fullname_cache = {}
        self.username_cache = {}
        
        # Memory stats
        self.last_memory_check = 0
        
        logger.info("Reddit tracker initialized")
        
    def _init_database(self):
        """Initialize the SQLite database for tracking posts"""
        try:
            os.makedirs(DATA_PATH, exist_ok=True)
            conn = sqlite3.connect(f'{DATA_PATH}/virality.db', check_same_thread=False)
            conn.row_factory = sqlite3.Row
            
            # Enable WAL mode for better concurrency
            conn.execute('PRAGMA journal_mode=WAL')
            
            # Create tables if they don't exist
            conn.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    author TEXT,
                    author_post_karma INTEGER,
                    author_comment_karma INTEGER,
                    author_account_age INTEGER,
                    subreddit TEXT,
                    created_utc INTEGER,
                    permalink TEXT,
                    url TEXT,
                    self_text TEXT,
                    is_self INTEGER,
                    is_video INTEGER,
                    is_original_content INTEGER,
                    over_18 INTEGER,
                    last_updated INTEGER,
                    tracking_interval INTEGER,
                    status TEXT,
                    archived_utc INTEGER,
                    initial_category TEXT,
                    flair_text TEXT,
                    flair_css_class TEXT,
                    domain TEXT,
                    is_gallery INTEGER,
                    is_crosspostable INTEGER,
                    pinned INTEGER,
                    spoiler INTEGER,
                    author_premium INTEGER,
                    poll_data TEXT
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS time_series (
                    id TEXT,
                    timestamp INTEGER,
                    score INTEGER,
                    upvote_ratio REAL,
                    num_comments INTEGER,
                    category TEXT,
                    num_awards INTEGER,
                    gilded INTEGER,
                    gildings TEXT,
                    all_awardings TEXT,
                    view_count INTEGER,
                    is_stickied INTEGER,
                    is_locked INTEGER,
                    contest_mode INTEGER,
                    crosspost_parent TEXT,
                    num_crossposts INTEGER,
                    num_duplicates INTEGER,
                    removed INTEGER,
                    removed_by_category TEXT,
                    distinguished TEXT,
                    PRIMARY KEY (id, timestamp)
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS subreddits (
                    name TEXT PRIMARY KEY,
                    display_name TEXT,
                    title TEXT,
                    description TEXT,
                    subscribers INTEGER,
                    created_utc INTEGER,
                    over18 INTEGER,
                    last_updated INTEGER
                )
            ''')
            
            conn.commit()
            return conn
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            sys.exit(1)
    
    def _setup_reddit_clients(self):
        """Set up multiple Reddit API clients for rotation"""
        for i in range(1, API_KEYS_COUNT + 1):
            client_id = os.getenv(f'REDDIT_CLIENT_ID_{i}')
            client_secret = os.getenv(f'REDDIT_CLIENT_SECRET_{i}')
            user_agent = os.getenv(f'REDDIT_USER_AGENT_{i}')
            
            if client_id and client_secret and user_agent:
                try:
                    # Create a more specific user agent
                    enhanced_user_agent = f"python:reddit-tracker:{i}.0 (by /u/RedditDataAnalyst)"
                    
                    reddit = praw.Reddit(
                        client_id=client_id,
                        client_secret=client_secret,
                        user_agent=enhanced_user_agent
                    )
                    
                    # Test the client with a simple request
                    try:
                        reddit.subreddit("announcements").display_name
                        self.reddit_clients.append({
                            'client': reddit, 
                            'id': i,
                            'remaining': 1000,
                            'reset_time': time.time() + 600,
                            'auth_failures': 0
                        })
                    except Exception:
                        # Silently ignore clients that fail authentication test
                        pass
                        
                except Exception:
                    # Silently ignore clients that can't be initialized
                    pass
        
        if not self.reddit_clients:
            logger.critical("No valid Reddit API clients found")
            sys.exit(1)
            
        logger.info(f"Successfully initialized {len(self.reddit_clients)} Reddit API clients")
    
    def _get_next_client(self):
        """Rotate to the next available Reddit client with exponential backoff"""
        # Start with the next client in rotation
        start_idx = self.current_client_idx
        attempts = 0
        
        while True:
            self.current_client_idx = (self.current_client_idx + 1) % len(self.reddit_clients)
            client_info = self.reddit_clients[self.current_client_idx]
            
            # If we've checked all clients and none are ready, wait
            if self.current_client_idx == start_idx:
                attempts += 1
                
                # Find the client that will be ready soonest
                earliest_reset = min(c['reset_time'] for c in self.reddit_clients)
                wait_time = max(0, earliest_reset - time.time() + 2)
                
                if wait_time > 0:
                    wait_time = min(wait_time, 5 * attempts)  # Cap max wait time
                    logger.warning(f"All API clients rate limited, waiting {wait_time:.1f} seconds")
                    time.sleep(wait_time)
                    continue
            
            # Check if this client is ready to use
            if client_info['auth_failures'] > MAX_AUTH_FAILURES:
                # Skip clients with too many auth failures
                continue
                
            if client_info['remaining'] > RATE_LIMIT_BUFFER or time.time() > client_info['reset_time']:
                return client_info['client'], self.current_client_idx
            
            # Otherwise try the next client
    
    def _update_rate_limits(self, client_idx):
        """Update the rate limit information for a client"""
        try:
            client_info = self.reddit_clients[client_idx]
            client = client_info['client']
            
            # Get current limits from the client
            remaining = client.auth.limits.get('remaining')
            reset_timestamp = client.auth.limits.get('reset_timestamp')
            
            if remaining is not None:
                client_info['remaining'] = remaining
                
            if reset_timestamp is not None:
                client_info['reset_time'] = reset_timestamp
                
            # Reset auth failures counter since the call succeeded
            client_info['auth_failures'] = 0
                
            # If we're getting close to the rate limit, log a warning
            if 0 < client_info['remaining'] <= RATE_LIMIT_BUFFER:
                reset_in = max(0, client_info['reset_time'] - time.time())
                logger.warning(
                    f"Client {client_idx+1} approaching rate limit: {client_info['remaining']} "
                    f"calls remaining, resets in {reset_in:.1f} seconds"
                )
                
            self.api_calls_count += 1
                
        except Exception:
            # Silently ignore rate limit update errors
            pass
    
    def safe_api_call(self, method_name, *args, **kwargs):
        """Make a safe API call with client rotation and rate limiting"""
        max_retries = 3
        retries = 0
        auth_failures = set()  # Track clients with auth failures
        
        while retries < max_retries and len(auth_failures) < len(self.reddit_clients):
            try:
                client, client_idx = self._get_next_client()
                
                # Skip clients we know are failing with auth issues
                if client_idx in auth_failures:
                    continue
                
                # Call the appropriate method based on method_name
                if method_name == "subreddit":
                    result = client.subreddit(args[0])
                elif method_name == "submission":
                    result = client.submission(id=args[0])
                elif method_name == "info":
                    result = client.info(fullnames=args[0])
                else:
                    return None
                
                # Update rate limit info
                self._update_rate_limits(client_idx)
                
                return result
                
            except praw.exceptions.ResponseException:
                # Mark this client as having auth failure
                auth_failures.add(client_idx)
                self.reddit_clients[client_idx]['auth_failures'] += 1
                
                # Just silently continue to next client
                continue
                
            except Exception:
                retries += 1
                time.sleep(2 * retries)  # Exponential backoff
                    
        # If we reach here, all attempts failed, but we don't log the error
        return None
    
    def _load_state(self):
        """Load tracking state from a plain text file"""
        try:
            self.state = {}  # Reset state
            
            if not os.path.exists(self.state_file):
                return
                
            with open(self.state_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                        
                    parts = line.split('|')
                    if len(parts) < 3:
                        continue
                        
                    # Parse the line based on its type
                    if parts[0] == 'sub':
                        # Format: sub|subreddit|category|last_id|checkpoint
                        if len(parts) >= 5:
                            subreddit = parts[1]
                            category = parts[2]
                            last_id = parts[3]
                            checkpoint = int(parts[4]) if parts[4].isdigit() else current_time()
                            
                            # Store in state dict
                            self.state[(subreddit, category)] = {
                                'last_id': last_id,
                                'checkpoint': checkpoint
                            }
                    elif parts[0] == 'meta':
                        # Format: meta|key|value
                        key = parts[1]
                        value = parts[2]
                        self.state[key] = value
                        
            logger.info(f"State loaded from {self.state_file}")
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            # Continue with empty state
            self.state = {}
    
    def _save_state(self, subreddit, category, last_id):
        """Save tracking state to a plain text file"""
        try:
            # Update the in-memory state
            state_key = (subreddit, category)
            
            if state_key not in self.state:
                self.state[state_key] = {}
                
            self.state[state_key]['last_id'] = last_id
            self.state[state_key]['checkpoint'] = current_time()
            
            # Write to disk occasionally to reduce I/O
            if random.random() < 0.1:  # 10% chance to write to disk
                self._write_state_to_disk()
                
        except Exception:
            # Silently ignore state saving errors
            pass
    
    def _write_state_to_disk(self):
        """Write the current state to the state file"""
        try:
            with open(self.state_file, 'w') as f:
                # Write subreddit tracking states
                for key, value in self.state.items():
                    if isinstance(key, tuple):
                        # It's a subreddit tracking state
                        subreddit, category = key
                        last_id = value.get('last_id', '')
                        checkpoint = value.get('checkpoint', current_time())
                        line = f"sub|{subreddit}|{category}|{last_id}|{checkpoint}\n"
                        f.write(line)
                    elif isinstance(key, str):
                        # It's a metadata entry
                        line = f"meta|{key}|{value}\n"
                        f.write(line)
        except Exception:
            # Silently ignore state writing errors
            pass
    
    def _fetch_subreddit_info(self, subreddit_name):
        """Fetch subreddit information and store in database"""
        try:
            sr = self.safe_api_call("subreddit", subreddit_name)
            if not sr:
                return False
                
            # Store in database
            now = current_time()
            with self.db_lock:
                self.conn.execute('''
                    INSERT OR REPLACE INTO subreddits 
                    (name, display_name, title, description, subscribers, created_utc, over18, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    subreddit_name.lower(),
                    sr.display_name,
                    getattr(sr, 'title', ''),
                    getattr(sr, 'public_description', ''),
                    getattr(sr, 'subscribers', 0),
                    getattr(sr, 'created_utc', 0),
                    int(getattr(sr, 'over18', False)),
                    now
                ))
                self.conn.commit()
                
            # Update cache
            self.subreddit_cache[subreddit_name] = {
                'display_name': sr.display_name,
                'subscribers': getattr(sr, 'subscribers', 0),
                'last_updated': now
            }
            
            return True
            
        except Exception:
            return False
    
    def _log_progress(self):
        """Log progress statistics"""
        try:
            with self.db_lock:
                # Count active posts
                active_posts = self.conn.execute(
                    'SELECT COUNT(*) FROM posts WHERE status = "active"'
                ).fetchone()[0]
                
                # Count archived posts
                archived_posts = self.conn.execute(
                    'SELECT COUNT(*) FROM posts WHERE status = "archived"'
                ).fetchone()[0]
                
                # Count time series entries
                time_series_entries = self.conn.execute(
                    'SELECT COUNT(*) FROM time_series'
                ).fetchone()[0]
                
            # Memory usage
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            logger.info(
                f"Stats: {active_posts} active posts, {archived_posts} archived, "
                f"{time_series_entries} time series entries, {self.api_calls_count} API calls, "
                f"{memory_mb:.1f} MB memory"
            )
            
        except Exception:
            # Silently ignore logging errors
            pass
    
    def _get_tracking_interval(self, created_utc):
        """Get the appropriate tracking interval based on post age"""
        post_age_hours = (current_time() - created_utc) / 3600
        
        for max_age, interval in sorted(AGE_TRACKING_INTERVALS.items()):
            if post_age_hours <= max_age:
                return interval
                
        # Default to the highest interval
        return AGE_TRACKING_INTERVALS[float('inf')]
    
    def _get_author_karma(self, author):
        """Get author karma and account age"""
        if not author or author == "[deleted]":
            return 0, 0, 0  # Default values for deleted authors
            
        try:
            if hasattr(author, 'link_karma'):
                post_karma = author.link_karma
                comment_karma = author.comment_karma
                account_age = int(time.time() - author.created_utc)
                return post_karma, comment_karma, account_age
        except:
            pass
            
        return 0, 0, 0  # Default values for error cases
    
    def _track_submission(self, submission, subreddit, category):
        """Track a single submission, updating or inserting as needed"""
        try:
            now = current_time()
            submission_id = submission.id
            
            # Check if we're already tracking this post
            with self.db_lock:
                existing = self.conn.execute(
                    'SELECT created_utc, tracking_interval, initial_category FROM posts WHERE id = ?', 
                    (submission_id,)
                ).fetchone()
                
            is_new = existing is None
                
            if is_new:
                # Only add new posts from the 'new' category
                if category != 'new':
                    return False
                    
                # Check post age - skip if older than 1 hour
                created_utc = submission.created_utc
                post_age_seconds = now - created_utc
                
                if post_age_seconds > 1200:  # 1200 seconds = 20 minutes
                    return False
                    
                # This is a new post, add it to our tracking
                tracking_interval = self._get_tracking_interval(created_utc)
                
                # Get flair information
                flair_text = getattr(submission, 'link_flair_text', None)
                flair_css = getattr(submission, 'link_flair_css_class', None)
                
                # Get domain information
                domain = getattr(submission, 'domain', '')
                
                # Check if it's a gallery
                is_gallery = int(getattr(submission, 'is_gallery', False))
                
                # Check if it's crosspostable
                is_crosspostable = int(getattr(submission, 'is_crosspostable', True))
                
                # Check if it's pinned/stickied
                pinned = int(getattr(submission, 'pinned', False))
                
                # Check if it's a spoiler
                spoiler = int(getattr(submission, 'spoiler', False))
                
                # Get author karma and account age
                author_post_karma = 0
                author_comment_karma = 0
                author_account_age = 0
                
                if submission.author:
                    author_post_karma, author_comment_karma, author_account_age = self._get_author_karma(submission.author)
                
                # Check if author has premium
                try:
                    author_premium = int(submission.author.is_gold if submission.author else False)
                except:
                    author_premium = 0
                
                # Get poll data if available
                poll_data = {}
                if hasattr(submission, 'poll_data') and submission.poll_data:
                    try:
                        poll_data = {
                            'total_vote_count': submission.poll_data.total_vote_count,
                            'voting_end_timestamp': submission.poll_data.voting_end_timestamp,
                            'options': [option.text for option in submission.poll_data.options]
                        }
                    except:
                        pass
                
                # Insert post data
                with self.db_lock:
                    self.conn.execute('''
                        INSERT INTO posts 
                        (id, title, author, author_post_karma, author_comment_karma, author_account_age,
                         subreddit, created_utc, permalink, url, 
                         self_text, is_self, is_video, is_original_content, over_18,
                         last_updated, tracking_interval, status, initial_category,
                         flair_text, flair_css_class, domain, is_gallery,
                         is_crosspostable, pinned, spoiler, author_premium, poll_data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        submission_id,
                        submission.title,
                        str(submission.author) if submission.author else "[deleted]",
                        author_post_karma,
                        author_comment_karma,
                        author_account_age,
                        subreddit,
                        created_utc,
                        submission.permalink,
                        submission.url,
                        submission.selftext if hasattr(submission, 'selftext') else '',
                        int(submission.is_self),
                        int(getattr(submission, 'is_video', False)),
                        int(getattr(submission, 'is_original_content', False)),
                        int(submission.over_18),
                        now,
                        tracking_interval,
                        'active',
                        category,
                        flair_text,
                        flair_css,
                        domain,
                        is_gallery,
                        is_crosspostable,
                        pinned,
                        spoiler,
                        author_premium,
                        json.dumps(poll_data) if poll_data else None
                    ))
                    self.conn.commit()
                    
                logger.info(f"Added new post {submission_id} from r/{subreddit}")
                
            else:
                # Update the tracking interval based on the post age
                created_utc = existing['created_utc']
                tracking_interval = self._get_tracking_interval(created_utc)
                
                # Update author karma if available
                author_post_karma = 0
                author_comment_karma = 0
                author_account_age = 0
                
                if submission.author:
                    author_post_karma, author_comment_karma, author_account_age = self._get_author_karma(submission.author)
                
                with self.db_lock:
                    self.conn.execute('''
                        UPDATE posts SET 
                        last_updated = ?, 
                        tracking_interval = ?,
                        author_post_karma = ?,
                        author_comment_karma = ?,
                        author_account_age = ?
                        WHERE id = ?
                    ''', (
                        now, 
                        tracking_interval, 
                        author_post_karma,
                        author_comment_karma,
                        author_account_age,
                        submission_id
                    ))
            
            # Collect all available metrics for time series
            
            # Get gildings information
            gildings_dict = {}
            if hasattr(submission, 'gildings'):
                gildings_dict = submission.gildings
                
            # Get awards information
            all_awardings = []
            if hasattr(submission, 'all_awardings'):
                try:
                    all_awardings = submission.all_awardings
                except:
                    pass
                    
            # Get view count if available
            view_count = getattr(submission, 'view_count', None)
            
            # Get crosspost information
            crosspost_parent = None
            if hasattr(submission, 'crosspost_parent') and submission.crosspost_parent:
                crosspost_parent = submission.crosspost_parent
                
            # Get number of crossposts
            num_crossposts = getattr(submission, 'num_crossposts', 0)
            
            # Get number of duplicates
            num_duplicates = getattr(submission, 'num_duplicates', 0)
            
            # Check if post is removed
            removed = 0
            removed_by_category = None
            if hasattr(submission, 'removed_by_category'):
                removed = 1 if submission.removed_by_category else 0
                removed_by_category = submission.removed_by_category
                
            # Get distinguished status (mod/admin/etc.)
            distinguished = getattr(submission, 'distinguished', None)
            
            # Add time series data
            with self.db_lock:
                self.conn.execute('''
                    INSERT OR REPLACE INTO time_series 
                    (id, timestamp, score, upvote_ratio, num_comments, category, num_awards,
                     gilded, gildings, all_awardings, view_count, is_stickied, is_locked, 
                     contest_mode, crosspost_parent, num_crossposts, num_duplicates,
                     removed, removed_by_category, distinguished)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    submission_id,
                    now,
                    submission.score,
                    submission.upvote_ratio if hasattr(submission, 'upvote_ratio') else 0,
                    submission.num_comments,
                    category,
                    len(all_awardings) if all_awardings else 0,
                    int(getattr(submission, 'gilded', 0)),
                    json.dumps(gildings_dict),
                    json.dumps(all_awardings) if all_awardings else None,
                    view_count,
                    int(getattr(submission, 'stickied', False)),
                    int(getattr(submission, 'locked', False)),
                    int(getattr(submission, 'contest_mode', False)),
                    crosspost_parent,
                    num_crossposts,
                    num_duplicates,
                    removed,
                    removed_by_category,
                    distinguished
                ))
                
                self.conn.commit()
                
            return True
            
        except Exception:
            return False

    def track_subreddit_category(self, subreddit, category):
        """Track posts from a specific subreddit and category"""
        try:
            # Get Reddit instance
            sr = self.safe_api_call("subreddit", subreddit)
            if not sr:
                return
                
            # Refresh subreddit info occasionally
            now = current_time()
            if subreddit not in self.subreddit_cache or (now - self.subreddit_cache[subreddit]['last_updated']) > 86400:
                self._fetch_subreddit_info(subreddit)
            
            # Get appropriate listing
            try:
                if category == 'new':
                    listing = sr.new(limit=50)
                elif category == 'hot':
                    listing = sr.hot(limit=50)
                elif category == 'rising':
                    listing = sr.rising(limit=30)
                elif category == 'top':
                    listing = sr.top(time_filter='day', limit=50)
                else:
                    return
            except Exception:
                return
                
            # Get the last processed post ID for this subreddit/category
            state_key = (subreddit, category)
            last_id = self.state.get(state_key, {}).get('last_id', '')
            
            # Process submissions
            processed = 0
            new_last_id = None
            
            # Get IDs of posts we're already tracking
            tracked_post_ids = set()
            if category != 'new':  # Only needed for non-'new' categories
                with self.db_lock:
                    rows = self.conn.execute('SELECT id FROM posts WHERE status = "active"').fetchall()
                    tracked_post_ids = {row['id'] for row in rows}
            
            for submission in listing:
                # Set new_last_id on first iteration
                if new_last_id is None:
                    new_last_id = submission.id
                    
                # Stop if we reach a previously processed post
                if submission.id == last_id:
                    break
                
                # For non-'new' categories, only track posts we've already seen
                if category != 'new' and submission.id not in tracked_post_ids:
                    continue  # Skip posts we haven't seen in 'new'
                    
                # Process this submission
                if self._track_submission(submission, subreddit, category):
                    processed += 1
                    
            # Update the checkpoint
            if new_last_id:
                self._save_state(subreddit, category, new_last_id)
            
            if processed > 0:
                logger.info(f"Processed {processed} posts from r/{subreddit}/{category}")
            
        except Exception:
            # Silently ignore subreddit tracking errors
            pass

    def batch_fetch_submissions(self, post_ids, batch_size=100):
        """Fetch multiple submissions in batches"""
        results = {}
        
        # Process in batches to avoid hitting API limits
        for i in range(0, len(post_ids), batch_size):
            batch = post_ids[i:i+batch_size]
            fullnames = [f"t3_{post_id}" for post_id in batch]
            
            submissions = self.safe_api_call("info", fullnames)
            if submissions:
                for submission in submissions:
                    results[submission.id] = submission
                    
            # Add a small delay between batches
            if i + batch_size < len(post_ids):
                time.sleep(1)
        
        return results

    def update_active_posts(self):
        """Update all active posts based on their tracking intervals"""
        try:
            now = current_time()
            
            # Get list of posts due for an update based on their tracking interval
            with self.db_lock:
                posts = self.conn.execute('''
                    SELECT id, last_updated, tracking_interval
                    FROM posts 
                    WHERE status = 'active'
                    AND last_updated < ?
                    ORDER BY last_updated
                    LIMIT 100
                ''', (now - 60,)).fetchall()  # Posts not updated in the last minute
            
            if not posts:
                return
                
            # Filter posts that are actually due for update based on their interval
            due_posts = []
            for post in posts:
                minutes_since_update = (now - post['last_updated']) / 60
                if minutes_since_update >= post['tracking_interval']:
                    due_posts.append(post['id'])
            
            if not due_posts:
                return
                
            # Batch fetch submissions
            logger.info(f"Fetching {len(due_posts)} posts for update")
            submissions = self.batch_fetch_submissions(due_posts)
            
            # Update each post
            updated = 0
            for post_id, submission in submissions.items():
                try:
                    # Get subreddit from database
                    with self.db_lock:
                        subreddit = self.conn.execute('SELECT subreddit FROM posts WHERE id = ?', 
                                                    (post_id,)).fetchone()['subreddit']
                    
                    if self._track_submission(submission, subreddit, 'update'):
                        updated += 1
                except Exception:
                    # Silently ignore individual post update errors
                    pass
            
            if updated > 0:
                logger.info(f"Updated {updated} active posts")
            
        except Exception:
            # Silently ignore update errors
            pass

    def archive_old_posts(self):
        """Archive posts older than the archive threshold"""
        try:
            now = current_time()
            archive_threshold = now - (ARCHIVE_AFTER_DAYS * 86400)
            
            with self.db_lock:
                # Find posts to archive
                to_archive = self.conn.execute('''
                    SELECT id 
                    FROM posts 
                    WHERE status = 'active' 
                    AND created_utc < ? 
                ''', (archive_threshold,)).fetchall()
                
                # Update their status
                if to_archive:
                    self.conn.execute('''
                        UPDATE posts 
                        SET status = 'archived', archived_utc = ? 
                        WHERE id IN (SELECT id FROM posts WHERE status = 'active' AND created_utc < ?)
                    ''', (now, archive_threshold))
                    self.conn.commit()
                    
                    logger.info(f"Archived {len(to_archive)} old posts")
        
        except Exception:
            # Silently ignore archiving errors
            pass

    def create_database_backup(self):
        """Create a compressed backup of the database"""
        try:
            backup_time = datetime.now().strftime("%Y%m%d_%H%M")
            backup_file = f"{DATA_PATH}/virality_backup_{backup_time}.db.gz"
            
            # Close any cursors and commit changes
            with self.db_lock:
                self.conn.commit()
            
            # Create backup in gzip format
            with open(f"{DATA_PATH}/virality.db", 'rb') as f_in:
                with gzip.open(backup_file, 'wb') as f_out:
                    f_out.write(f_in.read())
                    
            logger.info(f"Created database backup: {backup_file}")
            
            # Remove old backups
            self._cleanup_old_backups()
            
        except Exception:
            # Silently ignore backup errors
            pass
    
    def _cleanup_old_backups(self):
        """Remove backups older than BACKUP_DAYS"""
        try:
            now = time.time()
            backup_threshold = now - (BACKUP_DAYS * 86400)
            
            for file in os.listdir(DATA_PATH):
                if file.startswith("virality_backup_") and file.endswith(".db.gz"):
                    file_path = os.path.join(DATA_PATH, file)
                    if os.path.getmtime(file_path) < backup_threshold:
                        os.remove(file_path)
                        logger.info(f"Removed old backup: {file}")
                        
        except Exception:
            # Silently ignore cleanup errors
            pass

    def export_data_to_csv(self):
        """Export data to CSV for external analysis"""
        try:
            export_dir = os.path.join(DATA_PATH, 'exports')
            os.makedirs(export_dir, exist_ok=True)
            
            # Export active posts data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            
            # Get posts data
            with self.db_lock:
                df_posts = pd.read_sql_query('''
                    SELECT * FROM posts 
                    WHERE status = 'active' 
                    ORDER BY created_utc DESC
                ''', self.conn)
                
                # Get time series for active posts
                df_time_series = pd.read_sql_query('''
                    SELECT ts.* FROM time_series ts
                    JOIN posts p ON ts.id = p.id
                    WHERE p.status = 'active'
                    ORDER BY ts.timestamp DESC
                ''', self.conn)
            
            # Export to CSV
            df_posts.to_csv(f"{export_dir}/posts_{timestamp}.csv", index=False)
            df_time_series.to_csv(f"{export_dir}/time_series_{timestamp}.csv", index=False)
            
            logger.info(f"Exported data to CSV in {export_dir}")
            
        except Exception:
            # Silently ignore export errors
            pass

    def run_scheduler(self):
        """Run the tracking scheduler to continuously track Reddit posts"""
        try:
            logger.info("Starting Reddit tracker scheduler")
            scheduler = BlockingScheduler()
            
            # Schedule tracking for each subreddit/category
            for group_name, subreddits in SUBREDDIT_GROUPS.items():
                for subreddit in subreddits:
                    for category in CATEGORIES:
                        # Stagger the schedules to avoid hitting rate limits
                        minute_offset = hash(f"{subreddit}_{category}") % 10
                        scheduler.add_job(
                            self.track_subreddit_category,
                            'interval', 
                            minutes=10,
                            start_date=datetime.now() + timedelta(minutes=minute_offset),
                            args=[subreddit, category],
                            id=f"track_{subreddit}_{category}",
                            max_instances=1
                        )
                        
            # Schedule active post updates
            scheduler.add_job(
                self.update_active_posts,
                'interval',
                minutes=3,
                id="update_active_posts",
                max_instances=1,
                misfire_grace_time=120
            )
            
            # Schedule progress logging
            scheduler.add_job(
                self._log_progress,
                'interval',
                minutes=30,
                id="log_progress",
                max_instances=1
            )
            
            # Schedule archiving old posts
            scheduler.add_job(
                self.archive_old_posts,
                'interval',
                hours=6,
                id="archive_old_posts",
                max_instances=1
            )
            
            # Schedule database backups
            scheduler.add_job(
                self.create_database_backup,
                'interval',
                hours=24,
                id="create_database_backup",
                max_instances=1
            )
            
            # Schedule data export
            scheduler.add_job(
                self.export_data_to_csv,
                'interval',
                hours=12,
                id="export_data_to_csv",
                max_instances=1
            )
            
            # Start the scheduler
            logger.info("Scheduler started")
            scheduler.start()
            
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped")
        except Exception as e:
            logger.critical(f"Scheduler error: {e}")
            raise

    def cleanup(self):
        """Clean up resources before exit"""
        try:
            # Save state to disk (final save)
            self._write_state_to_disk()
                
            # Close database connection
            if self.conn:
                self.conn.close()
                
            logger.info("Cleaned up resources")
            
        except Exception:
            # Silently ignore cleanup errors
            pass


def main():
    """Main entry point"""
    try:
        # Initialize the tracker
        tracker = RedditTracker()
        
        # Log startup information
        logger.info(f"Reddit Tracker started - tracking {sum(len(subs) for subs in SUBREDDIT_GROUPS.values())} subreddits")
        
        # Run the scheduler
        tracker.run_scheduler()
        
    except KeyboardInterrupt:
        logger.info("Shutting down due to keyboard interrupt")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
    finally:
        # Clean up
        if 'tracker' in locals():
            tracker.cleanup()


if __name__ == "__main__":
    main()
