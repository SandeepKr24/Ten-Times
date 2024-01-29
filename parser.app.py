import json
import feedparser
import sqlite3
from datetime import datetime, timezone

# Function to fetch and parse RSS feeds
def fetch_rss(feed_url):
    feed = feedparser.parse(feed_url)
    articles = []

    for entry in feed.entries:
        title = entry.get("title", "")
        description = entry.get("summary", entry.get("description", ""))
        link = entry.get("link", "")
        
        published_str = entry.get("published", "")
        if published_str:
            try:
                published_at = datetime.strptime(published_str, "%a, %d %b %Y %H:%M:%S GMT")
            except ValueError:
                # Handle invalid date format gracefully
                published_at = datetime.now(timezone.utc)
        else:
            # If "published" field is empty or not present, use current date and time
            published_at = datetime.now(timezone.utc)

        article = {
            "title": title,
            "description": description,
            "link": link,
            "published_at": published_at
        }
        articles.append(article)

    return articles

# Function to create the news table if it doesn't exist
def create_table():
    conn = sqlite3.connect("news_database.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            link TEXT NOT NULL,
            published_at TIMESTAMP NOT NULL,
            category TEXT NOT NULL
        )
    ''')

    conn.commit()
    conn.close()

# Function to store articles in the database
def store_articles(articles, category):
    conn = sqlite3.connect("news_database.db")
    cursor = conn.cursor()

    for article in articles:
        cursor.execute(
            "INSERT INTO news (title, description, link, published_at, category) VALUES (?, ?, ?, ?, ?)",
            (article["title"], article["description"], article["link"], article["published_at"], category)
        )

    conn.commit()
    conn.close()

# Function to export articles to JSON
def export_to_json():
    conn = sqlite3.connect("news_database.db")
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM news")
    data = cursor.fetchall()

    json_data = [{"id": row[0], "title": row[1], "description": row[2], "link": row[3], "published_at": row[4], "category": row[5]} for row in data]

    with open("news_data.json", "w", encoding="utf-8") as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=2)

# Main function
def main():
    create_table()

    rss_feeds = [
        "http://rss.cnn.com/rss/cnn_topstories.rss",
        "http://qz.com/feed",
        "http://feeds.foxnews.com/foxnews/politics",
        "http://feeds.reuters.com/reuters/businessNews",
        "http://feeds.feedburner.com/NewshourWorld",
        "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml"
    ]

    for feed_url in rss_feeds:
        articles = fetch_rss(feed_url)
        store_articles(articles, "Others")  # Default category

    export_to_json()
    print("News articles fetched, stored, and exported to JSON successfully!")

if __name__ == "__main__":
    main()
