import os
import base64
import json
import re
import requests
import time
import functools
import sqlite3
import logging
import io
import concurrent.futures 
from datetime import datetime
import shutil
from PIL import Image, UnidentifiedImageError
from dotenv import load_dotenv

# Configuration 
load_dotenv()

# API key and endpoint
API_KEY = os.environ.get("GEMINI_API_KEY") 


MODEL_PRIORITY = [
    "gemini-2.5-flash-lite",
]

# Parallel Processing Config
MAX_WORKERS = 20  
SAVE_INTERVAL = 20 
REPROCESS_GROUPS = True 


# Setup paths relative to the script location
DATA_PARENT_DIR = r'C:\Users\sdogan'
DATA_PATH = os.path.join(DATA_PARENT_DIR, 'Reddit_Virality_Data')
MEDIA_DIR = os.path.join(DATA_PATH, 'media_files_2')
DB_PATH = os.path.join(DATA_PATH, 'virality.db')
OUTPUT_DIR = os.path.join(DATA_PATH, "API_results")
os.makedirs(OUTPUT_DIR, exist_ok=True) 
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "contextual_features.json")

# API Request Timeout (seconds)
REQUEST_TIMEOUT = 120
# Rate limit delay (seconds)
RATE_LIMIT_DELAY = 1 

# Logging Setup 
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Database Function
def load_titles(db_path):
    """Loads post IDs and titles from the SQLite database."""
    titles = {}
    if not os.path.exists(db_path):
        logging.error(f"Database file not found at {db_path}")
        return titles
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, title FROM posts")
        rows = cursor.fetchall()
        for post_id, title in rows:
            titles[str(post_id)] = title 
        logging.info(f"Loaded {len(titles)} titles from {db_path}")
    except sqlite3.Error as e:
        logging.error(f"Error loading titles from SQLite DB ({db_path}): {e}")
    except Exception as e:
        logging.error(f"Unexpected error loading titles: {e}")
    finally:
        if conn:
            conn.close()
    return titles

#  Updated Prompt
ENRICHED_FEATURE_PROMPT = """[SYSTEM PROMPT]
You are a professional meme analyst specializing in identifying factors that contribute to viral potential on platforms like Reddit. Analyze the provided media file(s) and the title.
If multiple files are provided (e.g., a video and an audio track), analyze them TOGETHER as a single cohesive piece of content.

Your goal is to extract descriptive features AND evaluate potential virality drivers using the specified categorical options ONLY.

**CRITICAL OUTPUT RULES:**
1. **Output purely valid JSON.**
2. **ESCAPE ALL INTERNAL DOUBLE QUOTES.** If a string value contains a quote, it MUST be escaped with a backslash. 
   - BAD: "text": "He said "Hello""
   - GOOD: "text": "He said \\"Hello\\""
3. Do not include newlines or control characters inside string values unless properly escaped.

**Analysis Steps:**
1. Identify key visual elements (people, objects, symbols).
2. Note text hierarchy and placement.
3. Recognize known meme templates.
4. For video/GIF, extract key frames.
5. For audio, evaluate tone.
6. Examine textual and visual references for cultural cues.
7. Describe layout and color composition.
8. Analyze language and references for the target audience.
9. Evaluate any offensive content.
10. Assess the severity and category of any offensive content.
11. Check the coherence between the post title and the media.
12. **Crucially:** Fill the 'virality_factors' section by selecting the *single best fit* from the provided categorical options for each factor. Use 'unknown' or 'none' judiciously if no category applies.

**Virality Factor Instructions (Use these exact options):**
{
  "virality_factors": {
    "content_message_related": {
      "description": "Content and message elements influencing virality.",
      "factors": {
        "humor_type": {
          "description": "The type of humor used in the meme.",
          "possible_values": ["relatable_everyday", "observational_quirky", "surreal_absurdist", "irony_sarcasm", "wordplay_pun", "dark_edgy", "wholesome_positive", "cringe_awkward", "niche_in_joke", "political_satire", "none", "multi_type"],
          "value_type": "categorical"
        },
        "emotional_resonance": {
           "description": "The primary shareable emotion evoked.",
           "possible_values": ["high_joy_amusement", "strong_relatability_empathy", "surprise_shock_intrigue", "nostalgia_sentimentality", "anger_outrage_frustration", "wholesome_inspiration_hope", "schadenfreude", "confusion_absurdity", "none", "multi_emotion"],
           "value_type": "categorical"
        },
        "relatability_score": { # Keep scale-based for nuance if useful
            "description": "How relatable is the core situation/feeling?",
            "possible_values": {"range": "1-5", "scale_definition": {"1": "Not relatable", "2": "Slightly", "3": "Moderately", "4": "Very", "5": "Highly relatable"}},
            "value_type": "ordinal_scale"
        },
        "insight_commentary_score": {
            "description": "Level of insight or social commentary offered.",
            "possible_values": {"range": "1-5", "scale_definition": {"1": "None", "2": "Minimal", "3": "Moderate", "4": "Significant", "5": "Highly insightful"}},
            "value_type": "ordinal_scale"
        },
        "controversy_provocation_score": {
            "description": "Level of potential controversy or provocation.",
            "possible_values": {"range": "1-5", "scale_definition": {"1": "Not controversial", "2": "Slightly", "3": "Moderately", "4": "Very", "5": "Extremely controversial"}},
            "value_type": "ordinal_scale"
        },
          "controversy_type": {
             "description": "The nature of the potential controversy.",
             "possible_values": ["political", "social_issue", "cultural_sensitivity", "fandom_dispute", "offensive_language_imagery", "none"],
             "value_type": "categorical"
          },
        "novelty_uniqueness_score": {
            "description": "Uniqueness of the meme's concept or format twist.",
             "possible_values": {"range": "1-5", "scale_definition": {"1": "Common/Derivative", "2": "Slight Twist", "3": "Moderately Novel", "4": "Very Unique", "5": "Highly Original"}},
            "value_type": "ordinal_scale"
        },
        "engagement_prompt": {
            "description": "Does the meme implicitly or explicitly prompt interaction?",
            "possible_values": ["none", "implicit_tag_share", "implicit_opinion_ask", "explicit_call_to_action", "fill_in_blank_template"],
            "value_type": "categorical"
        },
        "reference_specificity": {
             "description": "How niche or broad is the core reference/topic?",
             "possible_values": ["universal", "broad_cultural", "niche_subculture", "hyper_specific_event", "internet_meta", "none"],
             "value_type": "categorical"
          },
          "profanity_level": {
            "description": "Level of profanity in text/audio.",
            "possible_values": ["none", "mild", "moderate", "high", "explicit"],
            "value_type": "categorical"
          }
      }
    },
    "format_presentation_related": {
        "description": "Format and presentation elements influencing virality.",
        "factors": {
            "visual_clarity_impact": {
                "description": "How quickly and clearly is the visual message conveyed?",
                "possible_values": ["instantly_clear", "requires_short_focus", "requires_context_knowledge", "ambiguous_confusing"],
                "value_type": "categorical"
            },
            "perceived_effort": {
                "description": "Apparent effort in creating the meme.",
                "possible_values": ["low_effort_classic", "medium_effort_standard", "high_effort_edited", "remix_combination"],
                "value_type": "categorical"
            },
            "meme_format_category": {
              "description": "Primary format category.",
              "possible_values": ["image_macro_classic", "reaction_image_gif", "multi_panel_comic_strip", "exploitable_template", "video_clip_sound", "screenshot_based_(social_media_text)", "ai_generated_art_text", "art_drawing_original", "other", "unknown"],
              "value_type": "categorical"
            },
            "simplicity_score": { 
                "description": "How simple is the overall meme format and concept?",
                 "possible_values": {"range": "1-5", "scale_definition": {"1": "Very Complex", "2": "Complex", "3": "Moderate", "4": "Simple", "5": "Very Simple/Minimalist"}},
                "value_type": "ordinal_scale"
            },
            "visual_appeal_score": { 
                "description": "Subjective visual appeal or aesthetic quality.",
                "possible_values": {"range": "1-5", "scale_definition": {"1": "Low Appeal/Ugly", "2": "Slightly Appealing", "3": "Neutral/Average", "4": "Appealing", "5": "Highly Appealing/Visually Striking"}},
                "value_type": "ordinal_scale"
            }
        }
    },
    "social_contextual_related": {
        "description": "Social context and sharing motivation elements.",
        "factors": {
            "social_currency_proxy": {
              "description": "What social value does sharing this likely provide?",
              "possible_values": ["signals_humor_style", "signals_relatability_shared_experience", "signals_in_group_knowledge", "signals_opinion_stance", "signals_cultural_awareness", "simple_entertainment_mood_lift", "none", "multi_signal"],
              "value_type": "categorical"
            },
            "trend_status": {
                "description": "Relevance to current internet trends/formats.",
                "possible_values": ["hot_current_trend", "recent_trend", "recurring_classic_format", "niche_trend", "not_trend_related"],
                "value_type": "categorical"
            },
             "platform_of_origin": {
               "description": "Likely platform of origin if discernible.",
               "possible_values": ["reddit", "twitter_x", "facebook", "instagram", "imgur", "tiktok", "tumblr", "4chan", "9gag", "other_platform", "unknown_likely_oc"],
               "value_type": "categorical"
             },
             "shareability_indicator": {
               "description": "How easily shareable is the meme likely to be?",
               "possible_values": ["highly_shareable_broad", "shareable_within_niche", "less_shareable_specific_context", "format_optimized_for_sharing"],
               "value_type": "categorical"
             }
        }
    }
  }
}

POST TITLE: {title}

Output one JSON object adhering strictly to this streamlined structure. Focus on the most relevant categorical features for virality analysis. Use 'unknown' or 'none' appropriately. Prioritize selecting the best fit categorical value from the lists provided in 'virality_factors'. Ensure all keys from the requested structure below are present.
{
  "meme": "Concise description focusing on elements relevant to the meme's message.",
  "meme_type": "Categorical type of meme (e.g., Reaction, Exploitable, Image Macro).",
  "media_type": "image|gif|video|audio|unknown",
  "textual_content": {
    "extracted_text": ["List of text lines extracted", "Use OCR logic"],
    "sentiment_overall": "positive|negative|neutral|mixed|unknown",
    "language": "Language code (e.g., 'en') or name",
    "tone": "Overall tone (e.g., ironic, satirical, wholesome, aggressive)",
    "text_volume": "very_short|short|medium|long|very_long",
    "text_brevity": {
        "word_count": "integer",
        "character_count": "integer" }, 
    "text_image_alignment": "reinforcing|contrasting|complementary|unrelated|none",
    "confidence": "float (0.0-1.0)"
  },
  "visual_analysis": {
    "composition": "central_focus|multi_panel|rule_of_thirds|chaotic|simple_layout|unknown",
    "key_objects_primary": ["List dominant objects/subjects"],
    "identified_person": {
      "is_character": "true|false",
      "name": "Character name or 'unknown'",
      "is_celebrity": "true|false",
      "celebrity_name": "Celebrity name or 'unknown'",
      "confidence": "float (0.0-1.0)"
    },
    "facial_expression": {
      "is_face": "true|false",
      "primary_emotion": "happy|sad|angry|surprised|neutral|disgusted|fearful|contemptuous|none",
      "confidence": "float (0.0-1.0)"
    },
    "template": {
      "is_template": "true|false",
      "name": "Known template name or 'unknown'",
      "is_variant": "true|false"
    },
    "panels": "single|double|triple|quad|strip|other|unknown",
    "confidence": "float (0.0-1.0)"
  },
  "temporal_features": {
    "duration_seconds": "integer or 'unknown'",
    "audio_presence": "true|false",
    "audio_summary": "Music|Speech|Sound_Effects|Mixed|None",
    "confidence": "float (0.0-1.0)"
  },
  "contextual_features": {
    "cultural_reference_type": "Movie|TV_Show|Game|Political_Event|Internet_Culture|Stereotype|None",
    "target_audience": "Gamers|Millennials|Gen_Z|Political_Group|Specific_Fandom|General|Unknown",
    "primary_topic": "string (e.g., relationships, work, gaming, politics) or 'unknown'",
    "confidence": "float (0.0-1.0)"
  },
  "content_offensiveness": {
    "is_offensive": "true|false",
    "offense_type": "Sexist|Racist|HateSpeech|Violence|AdultContent|Other|None",
    "severity": "mild|moderate|high|none",
    "confidence": "float (0.0-1.0)"
  },
  "title": {
    "is_title_present": "true|false",
    "title_brevity": {
        "word_count": "integer",
        "character_count": "integer" },
    "title_sentiment": "positive|negative|neutral|mixed|unknown",
    "title_media_coherence": "synergistic|descriptive|contrasting|misleading|unrelated",
    "confidence": "float (0.0-1.0)"
  },
  "metadata": {
    "analysis_confidence": "float (0.0-1.0)",
    "ambiguous_features": ["List specific elements that were hard to interpret or 'none'"]
    # Other metadata added programmatically
  },
  "virality_factors": { # THIS SECTION REMAINS THE CORE - Keep as previously defined
      "content_message_related": {
        "humor_type": "string",
        "emotional_resonance": "string",
        "relatability_score": "integer (1-5)",
        "insight_commentary_score": "integer (1-5)",
        "controversy_provocation_score": "integer (1-5)",
        "controversy_type": "string",
        "novelty_uniqueness_score": "integer (1-5)",
        "engagement_prompt": "string",
        "reference_specificity": "string",
        "profanity_level": "string",
        "confidence": "float (0.0-1.0)"
      },
      "format_presentation_related": {
        "visual_clarity_impact": "string",
        "perceived_effort": "string",
        "meme_format_category": "string",
        "simplicity_score": "integer (1-5)",
        "visual_appeal_score": "integer (1-5)",
        "confidence": "float (0.0-1.0)"
      },
      "social_contextual_related": {
        "social_currency_proxy": "string",
        "trend_status": "string",
        "platform_of_origin": "string",
        "shareability_indicator": "string",
        "confidence": "float (0.0-1.0)"
      }
    }
}
"""

# Media Handling Functions
def encode_file_to_base64(filepath):
    """Encodes a file to base64."""
    try:
        if os.path.getsize(filepath) == 0:
            logging.warning(f"File is empty: {filepath}")
            return None
        with open(filepath, 'rb') as f:
            return base64.b64encode(f.read()).decode('utf-8')
    except FileNotFoundError:
        logging.error(f"File not found for encoding: {filepath}")
        return None
    except Exception as e:
        logging.error(f"Error encoding file {filepath}: {e}")
        return None

def get_media_type(filepath):
    """Determines media type from file extension."""
    try:
        ext = os.path.splitext(filepath)[1].lower().strip(".")
        if ext in ["png", "jpg", "jpeg"]:
            return "image"
        elif ext == "gif":
            return "gif"
        elif ext in ["mp4", "mov", "avi", "webm"]:
            return "video"
        elif ext in ["m4a", "mp3", "wav", "ogg"]:
            return "audio"
        else:
            return "unknown"
    except Exception as e:
        logging.error(f"Error getting media type for {filepath}: {e}")
        return "unknown"

def get_mime_type(filepath):
    """Determines MIME type from file extension."""
    try:
        ext = os.path.splitext(filepath)[1].lower().strip(".")
        mime_types = {
            "png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
            "gif": "image/gif", "mp4": "video/mp4", "mov": "video/quicktime",
            "avi": "video/x-msvideo", "webm": "video/webm", "m4a": "audio/mp4",
            "mp3": "audio/mpeg", "wav": "audio/wav", "ogg": "audio/ogg"
        }
        return mime_types.get(ext, "application/octet-stream")
    except Exception as e:
        logging.error(f"Error getting MIME type for {filepath}: {e}")
        return "application/octet-stream"

def get_media_dimensions(filepath, media_type):
    """Gets width and height for image/gif files."""
    if media_type in ["image", "gif"]:
        try:
            with Image.open(filepath) as img:
                img.seek(0)
                return {"width": img.width, "height": img.height}
        except (FileNotFoundError, UnidentifiedImageError):
             return {"width": "unknown", "height": "unknown"}
        except Exception as e:
            return {"width": "unknown", "height": "unknown"}
    else:
        return {"width": "N/A", "height": "N/A"} 

def extract_first_frame(filepath):
    """Extracts first frame from gif/video as JPEG base64."""
    try:
        with Image.open(filepath) as img:
            img.seek(0)
            rgb_im = img.convert('RGB')
            buffer = io.BytesIO()
            rgb_im.save(buffer, format="JPEG", quality=85)
            return base64.b64encode(buffer.getvalue()).decode('utf-8')
    except UnidentifiedImageError:
        logging.error(f"Cannot identify image file (corrupted or empty): {filepath}")
        return None
    except Exception as e:
        logging.error(f"Failed to extract first frame from {filepath}: {e}")
        return None

def create_low_res_jpeg_base64(filepath, max_dim=1024, quality=75):
    """Loads an image, rescales it, and re-encodes as JPEG."""
    try:
        with Image.open(filepath) as img:
            width, height = img.size
            if max(width, height) > max_dim:
                ratio = max_dim / max(width, height)
                new_size = (int(width * ratio), int(height * ratio))
                img = img.resize(new_size, Image.Resampling.LANCZOS)
            
            if img.mode != 'RGB':
                img = img.convert('RGB')
                
            buffer = io.BytesIO()
            img.save(buffer, format="JPEG", quality=quality)
            return base64.b64encode(buffer.getvalue()).decode('utf-8')
    except UnidentifiedImageError:
        logging.error(f"Cannot identify image file (corrupted or empty): {filepath}")
        return None
    except Exception as e:
        logging.error(f"Failed to create low-res JPEG fallback for {filepath}: {e}")
        return None


# JSON Processing
def clean_json_response(raw_text):
    """Attempts to extract and clean a JSON object from the raw API response."""
    if not raw_text: return None
    json_match = re.search(r'```json\s*(\{.*?\})\s*```', raw_text, re.DOTALL)
    if not json_match:
        json_match = re.search(r'(\{.*?\})', raw_text, re.DOTALL)

    if json_match:
        cleaned = json_match.group(1)
        try:
            cleaned = re.sub(r',\s*([\}\]])', r'\1', cleaned) 
            cleaned = re.sub(r'//.*?\n', '\n', cleaned) 
            cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL) 
            cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', cleaned)
            return cleaned.strip()
        except Exception as e:
            return json_match.group(1).strip()
    return None

def validate_features(features):
    """Basic validation: checks if top-level required sections exist."""
    required_sections = [
        "meme", "meme_type", "media_type", "textual_content", "visual_analysis",
        "temporal_features", "contextual_features", "content_offensiveness",
        "title", "metadata", "virality_factors"
    ]
    missing = [section for section in required_sections if section not in features]
    if missing:
        logging.warning(f"Validation Warning: Missing required sections: {missing}")
        return False 
    return True 

# API Interaction
def rate_limit(delay):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            logging.debug(f"Rate limiting: sleeping for {delay} seconds.")
            time.sleep(delay)
            return result
        return wrapper
    return decorator

@rate_limit(RATE_LIMIT_DELAY)
def process_media_group(filepaths, title="unknown", primary_filename=None):
    """
    Processes a list of media files (e.g., video + audio) as a single request.
    """
    if not filepaths_list_valid(filepaths):
        return None
    
    if primary_filename is None:
        primary_filename = os.path.basename(filepaths[0])

    logging.info(f"Processing group for: {primary_filename} ({len(filepaths)} files)")
    
    # 1. Prepare Content Parts
    content_parts = [{"text": ENRICHED_FEATURE_PROMPT.replace("{title}", str(title))}]
    
    media_metadata = []

    for filepath in filepaths:
        original_media_type = get_media_type(filepath)
        current_mime_type = get_mime_type(filepath)
        encoded_data = encode_file_to_base64(filepath)
        
        if not encoded_data and original_media_type in ['gif', 'video']:
            encoded_data = extract_first_frame(filepath)
            current_mime_type = "image/jpeg"
        
        if encoded_data:
            content_parts.append({
                "inlineData": {
                    "mimeType": current_mime_type,
                    "data": encoded_data
                }
            })
            media_metadata.append({
                "filename": os.path.basename(filepath),
                "type": original_media_type,
                "mime": current_mime_type
            })
        else:
            logging.warning(f"Failed to encode part: {filepath}")

    if len(content_parts) <= 1: # Only text part exists
        logging.error("No valid media parts could be encoded for this group.")
        return None

    # 2. Build Payload
    payload = {
        "contents": [{"parts": content_parts}],
        "generationConfig": {
             "temperature": 0.7, 
             "maxOutputTokens": 8192, 
        }
    }

    # 3. Call API
    for model in MODEL_PRIORITY:
        current_endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={API_KEY}"
        try:
            response = requests.post(current_endpoint, json=payload, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 429:
                time.sleep(5) 
                continue
            
            if response.status_code == 400:
                logging.warning(f"API 400 Error for group {primary_filename}: {response.text}")
                # Complex fallback for groups is hard; abort this specific group if it fails 400
                return None

            response.raise_for_status() 
            temp_data = response.json()
            
            # ... (Standard Parsing Logic) ...
            if not temp_data.get('candidates'): continue
            
            candidate = temp_data["candidates"][0]
            if "content" not in candidate: continue
            
            raw_output = candidate["content"]["parts"][0].get("text", "")
            if not raw_output: continue

            cleaned_json_str = clean_json_response(raw_output)
            if not cleaned_json_str: continue

            # Repair Loop
            features = None
            for r_try in range(20):
                try:
                    features = json.loads(cleaned_json_str, strict=False)
                    break 
                except json.JSONDecodeError as e:
                    error_char_idx = e.pos

                    cleaned_json_str = cleaned_json_str[:error_char_idx] + cleaned_json_str[error_char_idx+1:] 

            if features and validate_features(features):
                # Attach Metadata
                if "metadata" not in features: features["metadata"] = {}
                features["metadata"]["original_filename"] = primary_filename
                features["metadata"]["group_files"] = [m['filename'] for m in media_metadata]
                features["metadata"]["group_processing"] = True
                
                return {"features": features, "embedding": None}

        except Exception as e:
            logging.error(f"Error processing group {primary_filename}: {e}")
            continue

    return None

def filepaths_list_valid(filepaths):
    if not filepaths: return False
    return any(os.path.exists(fp) for fp in filepaths)

def main():
    logging.info("--- Meme Analysis: Group Processing Enabled ---")
    
    # ... (Backup logic same as before) ...
    if os.path.exists(OUTPUT_FILE):
        backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = os.path.join(os.path.dirname(OUTPUT_FILE), "backups")
        os.makedirs(backup_dir, exist_ok=True) 
        shutil.copy2(OUTPUT_FILE, os.path.join(backup_dir, f"backup_{backup_timestamp}.json.bak"))

    existing_results = {}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r') as f: existing_results = json.load(f)
        except: existing_results = {}

    titles = load_titles(DB_PATH)

    # --- GROUPING LOGIC ---
    files_by_id = {}
    all_files = os.listdir(MEDIA_DIR)
    for filename in all_files:
        filepath = os.path.join(MEDIA_DIR, filename)
        if os.path.isfile(filepath):
            pid = filename.split('.')[0]
            if pid not in files_by_id: files_by_id[pid] = []
            files_by_id[pid].append(filename)

    # Identify processing queue
    groups_to_process = []
    
    for pid, filenames in files_by_id.items():
        # Sort so primary file is first (mp4 > gif > img)
        def priority_key(fname):
            ext = os.path.splitext(fname)[1].lower()
            if ext == '.mp4': return 0
            if ext == '.gif': return 1
            if ext in ['.png', '.jpg', '.jpeg']: return 2
            return 3
        filenames.sort(key=priority_key)
        
        primary_file = filenames[0]
        full_paths = [os.path.join(MEDIA_DIR, f) for f in filenames]
        
        # Check if we should process this group
        should_process = False
        
        # 1. New ID
        if primary_file not in existing_results:
            should_process = True
        
        # 2. Reprocess Groups
        elif REPROCESS_GROUPS and len(filenames) > 1:
             # Check if we already did a group process (look for metadata flag)
             prev_data = existing_results.get(primary_file, {}).get('features', {}).get('metadata', {})
             if not prev_data.get('group_processing', False):
                 logging.info(f"Marking {pid} for re-processing (Combining {len(filenames)} files)")
                 should_process = True
        
        if should_process:
            groups_to_process.append({
                "paths": full_paths,
                "primary": primary_file,
                "title": titles.get(pid, "Unknown")
            })

    logging.info(f"Found {len(groups_to_process)} groups to process.")

    processed_count = 0
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {}
            for group in groups_to_process:
                future = executor.submit(process_media_group, group["paths"], group["title"], group["primary"])
                future_to_file[future] = group["primary"]

            for future in concurrent.futures.as_completed(future_to_file):
                fname = future_to_file[future]
                try:
                    result = future.result()
                    if result:
                        existing_results[fname] = result
                        processed_count += 1
                        logging.info(f"Finished: {fname}")
                        
                        if processed_count % SAVE_INTERVAL == 0:
                            with open(OUTPUT_FILE, 'w') as f: json.dump(existing_results, f, indent=2)
                except Exception as e:
                    logging.error(f"Error in future: {e}")

    except KeyboardInterrupt:
        logging.warning("Interrupted.")
    finally:
        with open(OUTPUT_FILE, 'w') as f: json.dump(existing_results, f, indent=2)

if __name__ == "__main__":
    main()