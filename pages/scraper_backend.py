"""
HubSpot Recipients Scraper - Backend Script
Runs independently from Streamlit to scrape recipient data
"""

from playwright.sync_api import sync_playwright
import json
import time
import os

USER_DATA_DIR = r"C:\Users\Srikumaran\AppData\Local\Google\Chrome\User Data\Default"

def build_hubspot_url(portal_id, email_id, ils_id):
    """Build HubSpot recipients URL with ILS ID"""
    # Using the same URL format as the original working scraper
    return f"https://app.hubspot.com/email/{portal_id}/details/{email_id}/recipients/sent?listId=null&ilsId={ils_id}"

def scrape_recipient_data(browser, portal_id, email_id, list_id, email_name, is_first=False):
    """Scrape recipient statistics for a specific email-list pair"""
    url = build_hubspot_url(portal_id, email_id, list_id)
    statistics_data = None
    page = None

    def handle_response(resp):
        nonlocal statistics_data
        try:
            resp_url = resp.url
            ct = resp.headers.get("content-type", "")

            if "application/json" in ct:
                try:
                    text = resp.text()
                    if not text:
                        return

                    data = json.loads(text)

                    # Capture statistics data
                    if "cosemail-stats/v1/details/statistics" in resp_url:
                        statistics_data = data

                except:
                    pass
        except:
            pass

    try:
        page = browser.new_page()

        # Bring page to front
        for _ in range(3):
            page.bring_to_front()
            time.sleep(0.3)

        page.on("response", handle_response)
        page.goto(url, wait_until="load", timeout=60000)

        # Keep window visible
        for _ in range(2):
            page.bring_to_front()
            time.sleep(0.3)

        # Wait 30 seconds for first URL, 10 seconds for subsequent URLs
        if is_first:
            print("    ⏳ First URL - waiting 30 seconds for initialization...")
            page.wait_for_timeout(30000)
        else:
            page.wait_for_timeout(10000)

        # CHECK IF SEGMENT IS UNAVAILABLE
        segment_unavailable = False
        try:
            # Look for "Segment unavailable" text on the page
            page_content = page.content()
            if "Segment unavailable" in page_content or "segment unavailable" in page_content.lower():
                segment_unavailable = True
                print(f"    ⚠️  WARNING: Segment unavailable - list {list_id} may have been deleted!")
        except Exception as e:
            print(f"    ⚠️  Could not check segment availability: {str(e)}")

        # Extract statistics
        if statistics_data:
            try:
                counters = statistics_data["aggregate"]["counters"]

                result = {
                    "email_name": email_name,
                    "email_id": email_id,
                    "list_id": list_id,
                    "sent": counters.get("sent", 0),
                    "delivered": counters.get("delivered", 0),
                    "opened": counters.get("open", 0),
                    "clicked": counters.get("click", 0),
                    "bounced": counters.get("bounce", 0),
                    "unsubscribed": counters.get("unsubscribed", 0),
                    "segment_unavailable": segment_unavailable  # Flag for deleted segments
                }

                # If segment is unavailable, mark it in the output
                if segment_unavailable:
                    print(f"    ⛔ SKIPPED - Segment deleted/unavailable (list may have been removed from HubSpot)")
                    return None  # Return None to skip this result

                return result
            except Exception as e:
                print(f"❌ Error extracting statistics for {email_name}: {str(e)}")
                return None

        return None

    except Exception as e:
        print(f"❌ Failed to scrape {email_name}: {str(e)}")
        return None

    finally:
        if page:
            try:
                page.close()
            except:
                pass

def main():
    """Main scraping function"""
    print("=" * 60)
    print("HubSpot Recipients Scraper - Backend")
    print("=" * 60)

    # Load associations from temp file
    temp_file = "temp_associations.json"
    if not os.path.exists(temp_file):
        print("❌ ERROR: temp_associations.json not found!")
        print("   Please click 'Start Scraping' in the Streamlit app first.")
        return

    with open(temp_file, "r") as f:
        data = json.load(f)

    portal_id = data["portal_id"]
    associations = data["associations"]

    print(f"\n📋 Loaded {len(associations)} associations to scrape")
    print(f"🌐 Portal ID: {portal_id}")
    print("\n🚀 Starting browser...\n")

    results = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch_persistent_context(
                USER_DATA_DIR,
                headless=False,
                no_viewport=True,
                args=['--start-maximized', '--window-position=0,0']
            )

            time.sleep(2)

            total = len(associations)

            for idx, assoc in enumerate(associations, 1):
                email_name = assoc["email_name"]
                list_name = assoc.get("list_name", "Unknown")

                print(f"[{idx}/{total}] Scraping: {email_name[:60]}...")

                # First URL gets 30 seconds, rest get 10 seconds
                is_first_url = (idx == 1)

                result = scrape_recipient_data(
                    browser,
                    portal_id,
                    assoc["email_id"],
                    assoc["list_id"],
                    email_name,
                    is_first=is_first_url
                )

                if result:
                    result["list_name"] = list_name
                    results.append(result)
                    print(f"    ✅ Success - Sent: {result['sent']}, Opened: {result['opened']}, Clicked: {result['clicked']}")
                else:
                    print(f"    ❌ Failed to capture data")

                if idx < total:
                    time.sleep(2)

            browser.close()

            print(f"\n{'='*60}")
            print(f"✅ Scraping Complete!")
            print(f"   Successfully scraped: {len(results)}/{total}")
            print(f"{'='*60}\n")

            # Save results
            results_file = "scraped_results.json"
            with open(results_file, "w") as f:
                json.dump(results, f, indent=2)

            print(f"💾 Results saved to: {results_file}")
            print(f"\n📌 Next steps:")
            print(f"   1. Go back to Streamlit app")
            print(f"   2. Click 'Load Scraped Results' button")
            print(f"   3. Export to Excel if needed\n")

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}\n")

        # Save partial results if any
        if results:
            results_file = "scraped_results.json"
            with open(results_file, "w") as f:
                json.dump(results, f, indent=2)
            print(f"💾 Partial results saved: {len(results)} items\n")

if __name__ == "__main__":
    main()
