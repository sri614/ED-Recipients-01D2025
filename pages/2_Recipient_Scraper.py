import streamlit as st
from pymongo import MongoClient
import re
from datetime import datetime, timedelta
import pandas as pd
import os
from dotenv import load_dotenv
import json
import subprocess
import sys
import requests

# Load environment variables
load_dotenv()

# ------------------ CONFIG ------------------
MONGODB_URI = os.getenv("MONGODB_URI")
PORTAL_ID = os.getenv("HUBSPOT_PORTAL_ID", "5686032")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")

client = MongoClient(MONGODB_URI)
db = client["test"]
lists_col = db["createdlists"]
emails_col = db["clonedemails"]

st.set_page_config(page_title="Recipient Scraper", layout="wide", initial_sidebar_state="collapsed")

# Custom CSS
st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    [data-testid="stSidebarNav"] {display: none;}

    .association-preview {
        background: #1e1e1e;
        border: 1px solid #444;
        border-radius: 8px;
        padding: 12px;
        margin: 8px 0;
    }

    .email-box {
        background: #2a2a4a;
        padding: 8px;
        border-radius: 6px;
        margin-bottom: 6px;
    }

    .list-box {
        background: #2a4a2a;
        padding: 8px;
        border-radius: 6px;
    }
    </style>
""", unsafe_allow_html=True)

st.title("📊 HubSpot Recipients Data Scraper")
st.markdown("Automatically scrape recipient statistics for associated email-list pairs")

# Clear cache button at the top
col1, col2, col3 = st.columns([3, 3, 4])
with col1:
    if st.button("🔄 Clear Cache & Reload", type="secondary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

with col2:
    st.markdown("👈 **Click if associations look wrong!**")

with col3:
    # Toggle for API validation
    validate_lists = st.checkbox("✅ Validate lists via HubSpot API", value=False,
                                 help="Check if lists still exist in HubSpot (slower but more accurate)")

st.warning("⚠️ **Important:** If you see mismatched dates in the preview cards, click 'Clear Cache & Reload' button above!")

# ------------------ HELPERS ------------------
def normalize_for_matching(text: str) -> str:
    """Remove ONLY Tier numbers for matching - keep everything else including dates and product names"""
    if not text:
        return ""

    original = text

    # Remove "Tier X" patterns (but keep everything else intact)
    # Pattern 1: "- Tier 1 -" or "- Tier 2 -"
    text = re.sub(r"-\s*Tier\s*\d+\s*-", "-", text, flags=re.IGNORECASE)
    # Pattern 2: "Tier 1" at the end or standalone
    text = re.sub(r"\s*-?\s*Tier\s*\d+\s*", " ", text, flags=re.IGNORECASE)

    # Normalize date format: convert "01 Dec" to "1 Dec" (remove leading zeros from day)
    # This handles patterns like "01 Dec 2025" -> "1 Dec 2025"
    text = re.sub(r'(\s|-)0(\d)\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)',
                  r'\1\2 \3', text, flags=re.IGNORECASE)

    # Clean up extra hyphens and spaces
    text = re.sub(r"\s*-\s*-\s*", " - ", text)  # Remove double hyphens
    text = re.sub(r"\s*-\s*$", "", text)  # Remove trailing hyphen
    text = re.sub(r"^\s*-\s*", "", text)  # Remove leading hyphen
    text = re.sub(r"\s+", " ", text)  # Normalize spaces

    return text.strip().lower()

def normalize_date(date_value):
    """Normalize different date formats to datetime object."""
    try:
        if isinstance(date_value, datetime):
            return date_value
        if isinstance(date_value, str):
            return datetime.fromisoformat(date_value.replace("Z", ""))
    except Exception:
        pass
    return None

def extract_date_from_name(name: str) -> datetime:
    """
    Extract date from email/list name like 'Campaign Name - 09 Nov 2025'
    Returns datetime object or None if no date found
    """
    if not name:
        return None

    # Match patterns like: "09 Nov 2025", "9 Nov 2025", "09 November 2025"
    month_names = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
        'january': 1, 'february': 2, 'march': 3, 'april': 4, 'june': 6,
        'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
    }

    pattern = r'(\d{1,2})\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|january|february|march|april|june|july|august|september|october|november|december)\s+(\d{4})'
    match = re.search(pattern, name, re.IGNORECASE)

    if match:
        day = int(match.group(1))
        month_str = match.group(2).lower()
        year = int(match.group(3))
        month = month_names.get(month_str)

        if month:
            try:
                return datetime(year, month, day)
            except ValueError:
                pass

    return None

# ------------------ HUBSPOT API VALIDATION ------------------
@st.cache_data(ttl=300)  # Cache for 5 minutes
def check_list_exists_in_hubspot(list_id: str) -> dict:
    """Check if a list exists in HubSpot using the API"""
    if not HUBSPOT_ACCESS_TOKEN or not list_id:
        return {"exists": True, "error": None}  # Skip validation if no token or list_id

    try:
        # HubSpot List API endpoint
        url = f"https://api.hubapi.com/contacts/v1/lists/{list_id}"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers, timeout=10)

        # If status is 200, list exists
        if response.status_code == 200:
            return {"exists": True, "error": None}
        # If 404, list doesn't exist (deleted)
        elif response.status_code == 404:
            return {"exists": False, "error": "List deleted from HubSpot"}
        else:
            # For other errors, assume list exists to avoid false positives
            return {"exists": True, "error": f"API returned {response.status_code}"}

    except Exception as e:
        # If API check fails, assume list exists (to avoid blocking valid lists)
        return {"exists": True, "error": str(e)}

# ------------------ FETCH AND BUILD ASSOCIATIONS (SAME AS PAGE 1) ------------------
@st.cache_data(ttl=60)
def get_associations(validate_with_api=False):
    """Fetch and build email-list associations - EXACT SAME LOGIC AS PAGE 1"""
    lists = list(lists_col.find({}, {
        "name": 1,
        "listId": 1,  # This IS the ILS ID for HubSpot
        "createdDate": 1,
        "_id": 0
    }).sort("createdDate", -1))

    emails = list(emails_col.find({}, {
        "clonedEmailName": 1,
        "clonedEmailId": 1,
        "createdAt": 1,
        "_id": 0
    }).sort("createdAt", -1))

    associations = []
    matched_list_ids = set()
    skipped_lists = []  # Track skipped lists for reporting

    # Match emails to lists - ONE EMAIL CAN MATCH MULTIPLE LISTS (for Tier 1, Tier 2, etc.)
    for email in emails:
        email_id = email.get("clonedEmailId")
        email_name = email.get("clonedEmailName", "")
        email_name_normalized = normalize_for_matching(email_name)

        # Extract date from email NAME (not MongoDB createdAt)
        email_date = extract_date_from_name(email_name)

        if not email_name_normalized:
            continue

        # Find ALL matching lists for this email (not just best match)
        matching_lists = []

        for lst in lists:
            list_id = lst.get("listId")
            if list_id in matched_list_ids:
                continue

            list_name = lst.get("name", "")
            list_name_normalized = normalize_for_matching(list_name)

            # Extract date from list NAME (not MongoDB createdDate)
            list_date = extract_date_from_name(list_name)

            if not list_name_normalized:
                continue

            # STRICT EXACT MATCHING - Email and List names must match exactly (after removing Tier)
            # This ensures 5Star emails only match 5Star lists, Fuse only matches Fuse, etc.

            # Calculate similarity score based on exact string match
            score = 0

            # Method 1: Check if normalized names are exactly the same
            if email_name_normalized == list_name_normalized:
                score = 100  # Perfect match
            # Method 2: Check if one is contained in the other (for slight variations)
            elif email_name_normalized in list_name_normalized or list_name_normalized in email_name_normalized:
                # Calculate how close they are
                shorter = min(len(email_name_normalized), len(list_name_normalized))
                longer = max(len(email_name_normalized), len(list_name_normalized))
                if shorter > 0:
                    similarity = (shorter / longer) * 100
                    # Only accept if very high similarity (95%+)
                    if similarity >= 95:
                        score = similarity

            # Require very high match (95%+) - this ensures exact matching
            if score < 95:
                continue  # Skip - not an exact match

            # STRICT DATE MATCHING - dates in names must match EXACTLY (same day only)
            if email_date and list_date:
                days_diff = abs((email_date.date() - list_date.date()).days)

                # Only consider matches if dates are EXACTLY the same day
                if days_diff != 0:
                    continue  # Skip this list if dates don't match exactly

                score += 5  # Same day - small boost
            else:
                # If either date is missing from name, skip this match
                continue

            matching_lists.append({
                "list": lst,
                "score": score
            })

        # Create associations for ALL matching lists
        for match in matching_lists:
            lst = match["list"]
            ils_id = lst.get("listId")

            # VALIDATE LIST EXISTS IN HUBSPOT (only if enabled)
            if validate_with_api:
                validation_result = check_list_exists_in_hubspot(str(ils_id))

                if not validation_result["exists"]:
                    # Skip this list - it was deleted from HubSpot
                    skipped_lists.append({
                        "list_name": lst.get("name"),
                        "list_id": str(ils_id),
                        "email_name": email_name,
                        "reason": validation_result.get("error", "List deleted from HubSpot")
                    })
                    continue

            associations.append({
                "email_name": email_name,
                "email_id": str(email_id),
                "list_name": lst.get("name"),
                "list_id": str(ils_id),  # This is the ILS ID
                "email_date": email_date,  # Date from email name
                "list_date": extract_date_from_name(lst.get("name", "")),  # Date from list name
                "match_score": match["score"]
            })
            matched_list_ids.add(ils_id)

    # Sort by date (most recent first)
    associations.sort(key=lambda x: x["email_date"] if x["email_date"] else datetime.min, reverse=True)

    return {"associations": associations, "skipped_lists": skipped_lists}

# ------------------ MAIN UI ------------------
result = get_associations(validate_with_api=validate_lists)
associations = result["associations"]
skipped_lists = result["skipped_lists"]

st.markdown(f"### 📋 Found {len(associations)} Associated Email-List Pairs")

# Show info about validation mode
if validate_lists:
    st.info("✅ API Validation is ENABLED - Checking each list against HubSpot API (slower but more accurate)")
    # Show warning if any lists were skipped
    if skipped_lists:
        st.warning(f"⚠️ {len(skipped_lists)} list(s) were skipped because they were deleted from HubSpot")
        with st.expander(f"🗑️ View {len(skipped_lists)} Skipped Lists (Deleted from HubSpot)"):
            skipped_df = pd.DataFrame(skipped_lists)
            skipped_df_display = skipped_df[["email_name", "list_name", "list_id", "reason"]].copy()
            skipped_df_display.columns = ["Email Name", "List Name", "List ID", "Reason"]
            st.dataframe(skipped_df_display, use_container_width=True)
            st.info("💡 These lists exist in your database but have been deleted from HubSpot. They will not be scraped.")
else:
    st.info("⚡ Fast Mode - API validation disabled. The scraper will check for deleted lists during scraping.")

if not associations:
    st.warning("⚠️ No associations found. Please create associations first in the Associated View page.")
else:
    st.markdown("---")

    # Date filter
    st.markdown("### 📅 Filter Associations by Date")

    col1, col2, col3 = st.columns([3, 3, 4])

    with col1:
        start_date = st.date_input("Start Date", value=datetime.now().date() - timedelta(days=7))
    with col2:
        end_date = st.date_input("End Date", value=datetime.now().date())

    # Apply date filter
    filtered_associations = []

    for assoc in associations:
        # Use email date for filtering
        email_date = assoc["email_date"]
        if not email_date:
            continue

        email_date_only = email_date.date()

        # Check if date is within the selected range
        if start_date <= email_date_only <= end_date:
            filtered_associations.append(assoc)

    st.markdown(f"**Selected for scraping:** {len(filtered_associations)} associations")

    # Display associations in card format (like page 1)
    if filtered_associations:
        st.markdown("---")
        st.markdown("### 💌 Matched Associations Preview")

        # Show first 10 associations as preview
        preview_count = min(10, len(filtered_associations))

        for i in range(0, preview_count, 2):
            cols = st.columns(2)

            for idx, assoc in enumerate(filtered_associations[i:i+2]):
                email_date = assoc["email_date"]
                date_str = email_date.strftime('%a, %b %d, %Y') if email_date else "N/A"

                with cols[idx]:
                    # Show list date in card to verify matching
                    list_date = assoc.get("list_date")
                    list_date_str = list_date.strftime('%d %b %Y') if list_date else "N/A"

                    st.markdown(f"""
                    <div class="association-preview">
                        <div style="text-align:center;margin-bottom:10px;color:#888;font-size:12px;">
                            Email Date: {date_str}
                        </div>
                        <div class="email-box">
                            <div style="color:#8888ff;font-weight:bold;font-size:11px;">📧 EMAIL</div>
                            <div style="font-size:12px;margin:4px 0;">{assoc['email_name']}</div>
                            <div style="color:#6366f1;font-size:11px;">ID: {assoc['email_id']}</div>
                        </div>
                        <div class="list-box">
                            <div style="color:#88ff88;font-weight:bold;font-size:11px;">📋 LIST</div>
                            <div style="font-size:12px;margin:4px 0;">{assoc['list_name']}</div>
                            <div style="color:#4ade80;font-size:11px;">ILS ID: {assoc['list_id']}</div>
                            <div style="color:#999;font-size:10px;margin-top:3px;">List Date: {list_date_str}</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

        if len(filtered_associations) > preview_count:
            st.info(f"Showing {preview_count} of {len(filtered_associations)} associations. All will be scraped.")

        # Show full table
        st.markdown("### 📊 Full Association List")
        df_filtered = pd.DataFrame(filtered_associations)
        df_filtered_display = df_filtered[["email_name", "email_id", "list_name", "list_id"]].copy()
        df_filtered_display.columns = ["Email Name", "Email ID", "List Name", "ILS List ID"]
        st.dataframe(df_filtered_display, width='stretch', height=300)

        st.info("ℹ️ Using ILS List IDs for HubSpot scraping (modern list IDs)")

        # Show sample URL that will be used
        if filtered_associations:
            sample_assoc = filtered_associations[0]
            sample_url = f"https://app.hubspot.com/email/{PORTAL_ID}/details/{sample_assoc['email_id']}/recipients/sent?listId=null&ilsId={sample_assoc['list_id']}"

            with st.expander("🔗 Sample Scraping URL (click to expand)"):
                st.code(sample_url, language="text")
                st.markdown(f"""
                **URL Components:**
                - Portal ID: `{PORTAL_ID}`
                - Email ID: `{sample_assoc['email_id']}`
                - ILS ID: `{sample_assoc['list_id']}`

                **Note:** The URL uses `listId=null&ilsId={sample_assoc['list_id']}` format.
                This is the correct HubSpot format for filtering by ILS segmented lists.
                """)

    st.markdown("---")

    # Check if results file exists and show status
    results_file = "scraped_results.json"
    results_available = os.path.exists(results_file)

    if results_available:
        file_time = os.path.getmtime(results_file)
        file_datetime = datetime.fromtimestamp(file_time)
        st.info(f"💾 Scraped results available from {file_datetime.strftime('%Y-%m-%d %H:%M:%S')} - Click 'Load Scraped Results' to view")

    # Scraping controls
    col1, col2, col3 = st.columns([2, 2, 6])

    with col1:
        if st.button("🚀 Start Scraping", type="primary", disabled=(len(filtered_associations) == 0)):
            if not filtered_associations:
                st.error("No associations selected!")
            else:
                # Save associations to temp file for scraper script
                temp_file = "temp_associations.json"
                associations_data = {
                    "portal_id": PORTAL_ID,
                    "associations": [
                        {
                            "email_id": a["email_id"],
                            "list_id": a["list_id"],
                            "email_name": a["email_name"],
                            "list_name": a["list_name"]
                        }
                        for a in filtered_associations
                    ]
                }

                with open(temp_file, "w") as f:
                    json.dump(associations_data, f, indent=2)

                st.success(f"✅ Prepared {len(filtered_associations)} associations for scraping")

                # Automatically run the scraper
                st.info("🚀 Starting automated scraper... Browser will open automatically.")

                try:
                    # Get the path to scraper_backend.py
                    scraper_path = os.path.join(os.getcwd(), "scraper_backend.py")

                    # Run the scraper in a new process (non-blocking)
                    if os.name == 'nt':  # Windows
                        # Open a new command window that stays visible
                        subprocess.Popen([
                            'start', 'cmd', '/k',
                            sys.executable, scraper_path
                        ], shell=True)
                    else:  # Linux/Mac
                        subprocess.Popen([
                            sys.executable, scraper_path
                        ])

                    st.success("✅ Scraper launched successfully!")
                    st.markdown("""
                    ### 📋 What's happening now:
                    1. ✅ A new command window has opened running the scraper
                    2. ⏳ Chrome browser will open and start scraping automatically
                    3. 📊 Watch the command window for progress updates
                    4. ⏱️ Wait for scraping to complete (you'll see "Scraping Complete!" message)
                    5. 🔄 Come back here and click **"📥 Load Scraped Results"** to view the data

                    💡 **Tip:** You can continue using this page while scraping runs in the background!
                    """)

                except Exception as e:
                    st.error(f"❌ Failed to start scraper: {str(e)}")
                    st.markdown("""
                    ### 🔧 Manual Fallback:
                    1. Open a **new terminal/command prompt**
                    2. Navigate to the project folder:
                       ```
                       cd "C:\\Users\\Srikumaran\\Desktop\\Datacapture DB"
                       ```
                    3. Run the scraper:
                       ```
                       python scraper_backend.py
                       ```
                    """)

    with col2:
        if st.button("🔄 Refresh"):
            st.cache_data.clear()
            st.rerun()

    # Load results button
    with col3:
        if st.button("📥 Load Scraped Results", use_container_width=True):
            results_file = "scraped_results.json"
            if os.path.exists(results_file):
                try:
                    with open(results_file, "r") as f:
                        results = json.load(f)
                        st.session_state.scraped_results = results

                        # Show file timestamp
                        file_time = os.path.getmtime(results_file)
                        file_datetime = datetime.fromtimestamp(file_time)
                        time_ago = datetime.now() - file_datetime

                        st.success(f"✅ Loaded {len(results)} results!")
                        st.info(f"📅 Results last updated: {file_datetime.strftime('%Y-%m-%d %H:%M:%S')} ({int(time_ago.total_seconds() / 60)} minutes ago)")
                        st.rerun()
                except Exception as e:
                    st.error(f"❌ Error loading results: {str(e)}")
            else:
                st.warning("⚠️ No results file found. Please wait for the scraper to complete, then try again.")

    # Display results if available
    if 'scraped_results' in st.session_state and st.session_state.scraped_results:
        st.markdown("---")
        st.markdown("### 📊 Scraped Results")

        df_results = pd.DataFrame(st.session_state.scraped_results)

        # Show raw data first (all rows)
        with st.expander("📋 View Raw Data (All Rows)", expanded=False):
            # Reorder columns: Email Name, Email ID, List Name, List ID, then 6 metrics
            column_order = [
                "email_name", "email_id", "list_name", "list_id",
                "sent", "delivered", "opened", "clicked", "bounced", "unsubscribed"
            ]

            existing_cols = [col for col in column_order if col in df_results.columns]
            df_raw = df_results[existing_cols].copy()

            # Rename for display
            display_names = {
                "email_name": "Email Name",
                "email_id": "Email ID",
                "list_name": "List Name",
                "list_id": "ILS List ID",
                "sent": "Sent",
                "delivered": "Delivered",
                "opened": "Opened",
                "clicked": "Clicked",
                "bounced": "Bounced",
                "unsubscribed": "Unsubscribed"
            }
            df_raw.columns = [display_names.get(col, col) for col in df_raw.columns]
            st.dataframe(df_raw, width='stretch')

        # Create grouped data organized by TIER number
        def extract_tier_number(list_name):
            """Extract tier number from list name like 'Campaign - Tier 1' or 'Campaign - Tier 2'"""
            if not list_name:
                return None
            match = re.search(r'tier\s*(\d+)', list_name, re.IGNORECASE)
            if match:
                return int(match.group(1))
            return None

        # Group by email and organize lists by tier
        grouped_data = {}
        for _, row in df_results.iterrows():
            email_id = row['email_id']
            email_name = row['email_name']
            tier_num = extract_tier_number(row.get('list_name', ''))

            if email_id not in grouped_data:
                grouped_data[email_id] = {
                    'email_name': email_name,
                    'email_id': email_id,
                    'tiers': {}  # Dictionary of tier -> list of lists
                }

            # Group by tier number
            if tier_num is not None:
                if tier_num not in grouped_data[email_id]['tiers']:
                    grouped_data[email_id]['tiers'][tier_num] = []

                # Append to this tier's list (handles multiple lists per tier)
                grouped_data[email_id]['tiers'][tier_num].append({
                    'list_name': row.get('list_name', ''),
                    'list_id': row.get('list_id', ''),
                    'sent': row.get('sent', 0),
                    'delivered': row.get('delivered', 0),
                    'opened': row.get('opened', 0),
                    'clicked': row.get('clicked', 0),
                    'bounced': row.get('bounced', 0),
                    'unsubscribed': row.get('unsubscribed', 0)
                })

        # Find max tiers and max lists per tier across all emails
        max_tier = 0
        max_lists_per_tier = {}
        for data in grouped_data.values():
            if data['tiers']:
                max_tier = max(max_tier, max(data['tiers'].keys()))
                for tier_num, tier_lists in data['tiers'].items():
                    max_lists_per_tier[tier_num] = max(
                        max_lists_per_tier.get(tier_num, 0),
                        len(tier_lists)
                    )

        # Convert to flat structure for Excel
        excel_data = []
        for email_id, data in grouped_data.items():
            row = {
                'Email Name': data['email_name'],
                'Email ID': data['email_id']
            }

            # Add columns for each tier (1, 2, 3, ...)
            for tier in range(1, max(max_tier + 1, 4)):  # At least show Tiers 1-3
                num_lists_in_tier = max_lists_per_tier.get(tier, 1)

                # For each list within this tier (handles multiple lists per tier)
                for list_idx in range(num_lists_in_tier):
                    tier_lists = data['tiers'].get(tier, [])

                    if list_idx < len(tier_lists):
                        list_data = tier_lists[list_idx]
                        prefix = f'Tier {tier}-{list_idx + 1}' if num_lists_in_tier > 1 else f'Tier {tier}'

                        row[f'{prefix} List Name'] = list_data.get('list_name', '')
                        row[f'{prefix} List ID'] = list_data.get('list_id', '')
                        row[f'{prefix} Sent'] = list_data.get('sent', 0)
                        row[f'{prefix} Delivered'] = list_data.get('delivered', 0)
                        row[f'{prefix} Opened'] = list_data.get('opened', 0)
                        row[f'{prefix} Clicked'] = list_data.get('clicked', 0)
                        row[f'{prefix} Bounced'] = list_data.get('bounced', 0)
                        row[f'{prefix} Unsubscribed'] = list_data.get('unsubscribed', 0)
                    else:
                        # Empty columns
                        prefix = f'Tier {tier}-{list_idx + 1}' if num_lists_in_tier > 1 else f'Tier {tier}'

                        row[f'{prefix} List Name'] = ''
                        row[f'{prefix} List ID'] = ''
                        row[f'{prefix} Sent'] = 0
                        row[f'{prefix} Delivered'] = 0
                        row[f'{prefix} Opened'] = 0
                        row[f'{prefix} Clicked'] = 0
                        row[f'{prefix} Bounced'] = 0
                        row[f'{prefix} Unsubscribed'] = 0

            excel_data.append(row)

        df_grouped = pd.DataFrame(excel_data)

        st.markdown("### 📊 Grouped by Email (One Row per Email)")
        st.dataframe(df_grouped, width='stretch')

        # Statistics
        st.markdown("### 📈 Summary Statistics")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Emails", len(df_grouped))
        with col2:
            sent_cols = [col for col in df_grouped.columns if 'Sent' in col and 'Tier' in col]
            total_sent = df_grouped[sent_cols].sum().sum() if sent_cols else 0
            st.metric("Total Sent", int(total_sent))
        with col3:
            opened_cols = [col for col in df_grouped.columns if 'Opened' in col and 'Tier' in col]
            total_opened = df_grouped[opened_cols].sum().sum() if opened_cols else 0
            st.metric("Total Opened", int(total_opened))
        with col4:
            clicked_cols = [col for col in df_grouped.columns if 'Clicked' in col and 'Tier' in col]
            total_clicked = df_grouped[clicked_cols].sum().sum() if clicked_cols else 0
            st.metric("Total Clicked", int(total_clicked))

        # Export button
        st.markdown("---")
        col1, col2 = st.columns([2, 8])
        with col1:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"recipient_data_{timestamp}.xlsx"

            # Save to Excel with grouped format
            df_grouped.to_excel(filename, index=False, sheet_name='Recipients')

            with open(filename, "rb") as file:
                st.download_button(
                    label="📥 Download Excel File",
                    data=file,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )

        st.success(f"✅ Ready to export! Excel organized by TIER with one row per email (Tier 1, Tier 2, etc.)")

client.close()
