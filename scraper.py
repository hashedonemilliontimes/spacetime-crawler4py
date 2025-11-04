import re
import json
import os
from threading import RLock
from urllib.parse import urlparse, urljoin, urldefrag
from collections import Counter, defaultdict
from bs4 import BeautifulSoup

seen_urls = set()
seen_urls_lock = RLock()  # Lock for thread-safe access to seen_urls

### analytics for report ###

ANALYTICS_FILE = "analytics.json"

word_freq = Counter() # for 50 most common words
longest_page = {"url": None, "words": 0}
subdomain_pages = defaultdict(set)
analytics_lock = RLock()  # Lock for thread-safe access to analytics variables

# Thresholds for page filtering
DEAD_PAGE_THRESHOLD = 10  # Pages with fewer than this many words are considered dead
LOW_INFO_THRESHOLD = 30   # Pages with fewer than this many words are considered low information

############################

## Analytics Persistence Functions ##

def load_analytics():
    """Load analytics from JSON file if it exists."""
    global word_freq, longest_page, subdomain_pages
    if os.path.exists(ANALYTICS_FILE):
        try:
            with open(ANALYTICS_FILE, 'r') as f:
                data = json.load(f)
                word_freq = Counter(data.get('word_freq', {}))
                longest_page = data.get('longest_page', {"url": None, "words": 0})
                # Convert subdomain_pages from dict of lists to dict of sets
                subdomain_pages = defaultdict(set)
                for domain, urls in data.get('subdomain_pages', {}).items():
                    subdomain_pages[domain] = set(urls)
        except Exception as e:
            print(f"Error loading analytics: {e}")

def save_analytics():
    """Save analytics to JSON file (thread-safe)."""
    global word_freq, longest_page, subdomain_pages
    try:
        # Thread-safe read: lock while reading analytics data to get consistent snapshot
        with analytics_lock:
            # Convert data to JSON-serializable format
            data = {
                'word_freq': dict(word_freq),
                'longest_page': longest_page.copy(),  # Copy dict to avoid reference issues
                'subdomain_pages': {domain: list(urls) for domain, urls in subdomain_pages.items()}
            }
        # Write outside lock to minimize lock hold time
        with open(ANALYTICS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error saving analytics: {e}")

# Load analytics on module import
load_analytics()

############################

# check if URL is from allowed UCI domains
allowed_domains = [
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu"
]

stopwords = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an",
    "and", "any", "are", "aren't", "as", "at", "be", "because", "been",
    "before", "being", "below", "between", "both", "but", "by", "can't",
    "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
    "doing", "don't", "down", "during", "each", "few", "for", "from",
    "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having",
    "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself",
    "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
    "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself",
    "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor",
    "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our",
    "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
    "then", "there", "there's", "these", "they", "they'd", "they'll",
    "they're", "they've", "this", "those", "through", "to", "too", "under",
    "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're",
    "we've", "were", "weren't", "what", "what's", "when", "when's", "where",
    "where's", "which", "while", "who", "who's", "whom", "why", "why's",
    "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're",
    "you've", "your", "yours", "yourself", "yourselves"
}


## Helper Functions for Grabbing Analytics ##

def extract_words_from_html(content: bytes):
    try:
        soup = BeautifulSoup(content, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        words = re.split(r"\W+", text)
        return [w.lower() for w in words if w and w.isascii()]
    except Exception as e:
        print("Word-extract error: ", e)
        return []


def update_analytics(url: str, words):
    """Thread-safe update of analytics data."""
    global word_freq, longest_page, subdomain_pages
    
    with analytics_lock:
        for w in words:
            if w not in stopwords:
                word_freq[w]+=1
        
        count = len(words)
        if count > longest_page["words"]:
            longest_page["words"] = count
            longest_page["url"] = url
        
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if host.endswith(".uci.edu") or host == "uci.edu":
            subdomain_pages[host].add(url)
    
    # Save analytics periodically (every 100 pages or on every update for safety)
    # Since we can't test, save frequently to avoid data loss
    save_analytics()

###################


def scraper(url, resp):
    global seen_urls

    if resp.status != 200 or not resp.raw_response or not resp.raw_response.content:
        return []
    
    # Extract words from the page content
    words = extract_words_from_html(resp.raw_response.content)
    word_count = len(words)
    
    # Dead page detection: Skip pages with very few words (200 status but no meaningful content)
    if word_count < DEAD_PAGE_THRESHOLD:
        return []  # Skip dead pages - they return 200 but have no data
    
    # Low information page filtering: Skip pages with minimal content (navigation-only pages)
    # This helps avoid crawling sets of similar pages with no information
    if word_count < LOW_INFO_THRESHOLD:
        return []  # Skip low information pages (mostly navigation/menus with no real content)
    
    # Update analytics for pages that pass the filters
    update_analytics(url, words)

    links = extract_next_links(url, resp)
    valid_links = []
    
    for link in links:

        link, _ = urldefrag(link) # make sure fragment is gone before checking

        # Thread-safe check and add to seen_urls
        with seen_urls_lock:
            if link not in seen_urls and is_valid(link):
                seen_urls.add(link)
                valid_links.append(link)

    return valid_links

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if resp.status != 200:
        return []
    
    # check if we have content
    if not resp.raw_response or not resp.raw_response.content:
        return []
    
    try:
        # parse the HTML content
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        links = []
        
        # find all anchor tags with href attributes
        for link in soup.find_all('a', href=True):
            href = link['href']
            # convert relative URLs to absolute URLs
            absolute_url = urljoin(url, href)
            # remove fragment part (everything after #)
            if '#' in absolute_url:
                absolute_url = absolute_url.split('#')[0]
            links.append(absolute_url)
        
        return links
    except Exception as e:
        print(f"Error parsing HTML for {url}: {e}")
        return []

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        url, _ = urldefrag(url)
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
    
        domain = parsed.netloc.lower()
        if not any(domain == d or domain.endswith("." + d) for d in allowed_domains):
            return False

        # Crawler Trap Protections
        if len(url) > 2000:
            return False
        if url.count('/') > 20:
            return False
        if len(parsed.query) > 100:
            return False
    
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
