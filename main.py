import concurrent.futures
import feedparser
import email.utils
from datetime import datetime, timedelta, timezone
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging
from urllib.parse import quote_plus
from llm import choose_relevant_niches
import streamlit as st
import pandas as pd
import io


NITTER_INSTANCE = "https://nitter.space"
TRUMP_LINK = "https://truthsocial.com/@realDonaldTrump"


def get_chrome_options(headless=True):
    """Create and return ChromeOptions with pre-configured settings."""
    options = webdriver.ChromeOptions()

    # Set DNS over HTTPS configuration
    # local_state = {
    #     "dns_over_https.mode": "secure",
    #     "dns_over_https.templates": "https://chrome.cloudflare-dns.com/dns-query",
    # }
    # options.add_experimental_option('localState', local_state)

    # Configure user agent
    user_agent = ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36')
    options.add_argument(f'user-agent={user_agent}')

    if headless:
        options.add_argument("--headless")

    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")  # Required for Streamlit Cloud
    options.add_argument("--disable-dev-shm-usage")  # Use disk instead of memory
    options.add_argument("--disable-extensions")  # Reduce memory usage
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--window-size=1280,800")  # Avoid viewport errors
    return options


def create_driver(options):
    """Initialize and return a new Chrome webdriver."""
    driver = webdriver.Chrome(options=options)
    return driver


def scrape_nitter(keyword, max_tweets=10):
    """
    Scrape Nitter search results using Selenium.

    Args:
        keyword (str): Search keyword.
        max_tweets (int): Maximum number of tweets to retrieve.

    Returns:
        list: List of tweet texts.
    """
    encoded_keyword = quote_plus(keyword)
    search_url = f"{NITTER_INSTANCE}/search?f=tweets&q={encoded_keyword}+usa"

    options = get_chrome_options(headless=True)
    driver = create_driver(options)

    tweets = []
    st.write(f"Scraping tweets for: **{keyword}**")

    try:
        driver.get(search_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.tweet-content"))
        )
        tweet_elements = driver.find_elements(By.CSS_SELECTOR, "div.tweet-content")

        tweet_display = st.empty()  # Placeholder for updating UI
        for i, tweet in enumerate(tweet_elements):
            if i >= max_tweets:
                break
            tweets.append(tweet.text)
            tweet_display.write(f"✅ {i + 1}/{max_tweets} tweets fetched...")  # Live progress update
            time.sleep(0.5)  # Simulate live updates

    except Exception as e:
        st.error(f"Error scraping Nitter: {str(e)}")
        logging.error(f"Error scraping Nitter: {str(e)}")
    finally:
        driver.quit()

    return tweets


def fetch_feed(query, days=5):
    """
    Fetch the RSS feed for a given query and filter out news older than 'days' days.

    Args:
        query (str): The search query.
        days (int): Number of days to look back.

    Returns:
        tuple: (query, list of filtered feed entries)
    """
    # Use quote_plus to safely encode the query
    encoded_query = quote_plus(query)
    rss_url = f"https://news.google.com/rss/search?q={encoded_query}+usa"
    feed = feedparser.parse(rss_url)
    filtered_entries = []

    current_time = datetime.now(timezone.utc)
    cutoff_time = current_time - timedelta(days=days)

    st.write(f"Fetching news for: **{query}**")
    news_display = st.empty()  # Placeholder for updating UI

    for entry in feed.entries:
        try:
            published_date = email.utils.parsedate_to_datetime(entry.published)
        except Exception as e:
            st.warning(f"Could not parse date for entry: {entry.get('title', 'No Title')}")
            continue

        if published_date > cutoff_time:
            filtered_entries.append({
                "title": entry.title,
                "link": entry.link,
                "date": entry.published
            })
            news_display.write(f"📰 {len(filtered_entries)} articles fetched...")  # Live update
            time.sleep(0.5)  # Simulate processing delay

    return query, filtered_entries


def scroll_up_until_elements(driver, selector, min_count=10, max_scrolls=15):
    """
    Scrolls the page until at least min_count unique elements (by aria-label) are found.

    Args:
        driver (webdriver): Selenium webdriver instance.
        selector (str): CSS selector to find elements.
        min_count (int): Minimum number of unique elements required.
        max_scrolls (int): Maximum number of scroll attempts.

    Returns:
        list: List of unique aria-label texts.
    """
    unique_texts = set()
    body = driver.find_element(By.TAG_NAME, "body")

    for _ in range(max_scrolls):
        elements = driver.find_elements(By.CSS_SELECTOR, selector)
        for element in elements:
            aria_label = element.get_attribute("aria-label")
            if aria_label:
                unique_texts.add(aria_label)
        if len(unique_texts) >= min_count:
            break
        body.send_keys(Keys.PAGE_DOWN)
        time.sleep(2)  # Allow time for new elements to load
    return list(unique_texts)


def trump_scraper():
    """
    Scrapes posts from Donald Trump's Truth Social page using Selenium.

    Returns:
        list: List of post aria-label texts.
    """
    options = get_chrome_options(headless=True)
    driver = create_driver(options)
    posts = []
    try:
        driver.get(TRUMP_LINK)
        # Wait until the timeline element is loaded
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#timeline"))
        )
        posts = scroll_up_until_elements(driver, "#timeline .status[aria-label]", min_count=10)
    except Exception as e:
        logging.error(f"Error scraping Trump page: {e}")
    finally:
        driver.quit()
    return posts


def convert_json_to_csv(json_data):
    data_list = []
    if "List of Affected Business Categories" in json_data:
        for item in json_data["List of Affected Business Categories"]:
            data_list.append({
                "Business Category Name": item["Business Category Name"],
                "NAIC Code": item["NAIC Code"],
                "Affected Commodities": ", ".join(item["Affected Commodities"]),
                "Potential Impact": item["Potential Impact"]
            })
    print(data_list)
    df = pd.DataFrame(data_list)
    return df


def save_scrapes_to_excel(combined_data):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Process news_feeds: Flatten data from each category
        news_rows = []
        for category, items in combined_data.get("news_feeds", {}).items():
            for item in items:
                news_rows.append({
                    "category": category,
                    "title": item.get("title"),
                    "link": item.get("link"),
                    "date": item.get("date")
                })
        df_news = pd.DataFrame(news_rows)
        df_news.to_excel(writer, sheet_name="News Feeds", index=False)

        # Process x_tweets: Each category contains tweet strings
        tweets_rows = []
        for category, tweets in combined_data.get("x_tweets", {}).items():
            for tweet in tweets:
                tweets_rows.append({
                    "category": category,
                    "tweet": tweet
                })
        df_tweets = pd.DataFrame(tweets_rows)
        df_tweets.to_excel(writer, sheet_name="Tweets", index=False)

        # Process trump_data: Each category contains tweet strings
        trump_rows = []
        for category, tweets in combined_data.get("trump_data", {}).items():
            for tweet in tweets:
                trump_rows.append({
                    "category": category,
                    "tweet": tweet
                })
        df_trump = pd.DataFrame(trump_rows)
        df_trump.to_excel(writer, sheet_name="Trump Tweets", index=False)

    # Ensure the buffer's pointer is at the beginning before returning
    output.seek(0)
    return output.getvalue()


def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    feed_results = {}
    tweets_data = {}
    trump_data = {}

    # Streamlit UI
    st.title("News & Tweets Scraper")
    keywords = st.text_area("Enter keywords (comma-separated):")

    if st.button("Fetch Data"):
        # Clear stored outputs so that previous results disappear
        for key in ["response_json", "csv_data", "scrapes_excel"]:
            if key in st.session_state:
                del st.session_state[key]
        keyword_list = [kw.strip() for kw in keywords.split(",") if kw.strip()]
        if keyword_list:
            with st.spinner("Fetching data..."):
                status = st.status("Processing queries...", expanded=True)
                # Fetch news feeds in parallel (I/O-bound)
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_query = {executor.submit(fetch_feed, query): query for query in keyword_list}
                    for future in concurrent.futures.as_completed(future_to_query):
                        query = future_to_query[future]
                        try:
                            query_key, entries = future.result()
                            feed_results[query_key] = entries
                            status.update(label=f"✅ News fetched for {query}")
                        except Exception as exc:
                            logging.error(f"Query '{query}' generated an exception: {exc}")
                            status.update(label=f"⚠️ Error fetching news for {query}")
                print(feed_results)
                # Scrape tweets from Nitter sequentially
                for query in keyword_list:
                    try:
                        tweets = scrape_nitter(query)
                        tweets_data[query] = tweets
                        status.update(label=f"✅ Tweets fetched for {query}")
                    except Exception as exc:
                        logging.error(f"Error scraping Nitter for '{query}': {exc}")

                trump_data["Donald Trump Tweets"] = trump_scraper()
                status.update(label="✅ Trump's tweets scraped")
                status.update(label="✅ All data fetched! Passing it to AI for 💭", state="complete")

                # Combine results into a single dictionary
                combined_data = {
                    "news_feeds": feed_results,
                    "x_tweets": tweets_data,
                    "trump_data": trump_data
                }

                # Write results to a JSON file
                output_filename = "data.json"
                try:
                    with open(output_filename, "w", encoding="utf-8") as file:
                        json.dump(combined_data, file, ensure_ascii=False, indent=4)
                    logging.info(f"Data successfully written to {output_filename}")
                except Exception as e:
                    logging.error(f"Failed to write data to file: {e}")

                # Process combined data for AI or further analysis
                response_json = choose_relevant_niches(combined_data)
                st.session_state.response_json = response_json
                # st.json(response_json)

                # Convert JSON to CSV and store in session_state
                df = convert_json_to_csv(response_json)
                csv_data = df.to_csv(index=False).encode("utf-8")
                st.session_state.csv_data = csv_data

                # Create Excel data and store in session_state
                scrapes_excel = save_scrapes_to_excel(combined_data)
                st.session_state.scrapes_excel = scrapes_excel
        else:
            st.error("Please enter at least one keyword.")
    # Check if response data already exists in session state
    if "response_json" in st.session_state:
        st.json(st.session_state.response_json)

    # Render download buttons if data exists in session_state
    if "csv_data" in st.session_state and "scrapes_excel" in st.session_state:
        st.download_button(
            "Download CSV with identified categories",
            st.session_state.csv_data,
            "scraped_data.csv",
            "text/csv"
        )
        st.download_button(
            label="Download Scraped News/Tweets",
            data=st.session_state.scrapes_excel,
            file_name="scraped_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


if __name__ == "__main__":
    main()
