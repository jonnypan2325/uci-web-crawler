import re
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from bs4 import BeautifulSoup
import os
import json
import atexit
from collections import defaultdict
import threading

analytics = "analytics.json"
save_interval = 50
pages_crawled_since_prev_save = 0
analytics_lock = threading.Lock()

# stop words from: https://www.ranks.nl/stopwords (referenced on Canvas)
STOP_WORDS = frozenset([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any",
    "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below",
    "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did",
    "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few",
    "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't",
    "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself",
    "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in",
    "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most",
    "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or",
    "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't",
    "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than",
    "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
    "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those",
    "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd",
    "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's",
    "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with",
    "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves",
])

def load_analytics():
    global unique_pages, longest_page, subdomains, word_counts

    word_counts = defaultdict(int)
    longest_page = ("", 0)
    subdomains = defaultdict(set)
    unique_pages = set()

    if not os.path.exists(analytics):
        return
    try:
        with open(analytics, "r") as f:
            data = json.load(f)
            unique_pages = set(data.get("unique_pages", []))
            longest_page = tuple(data.get("longest_page", ("", 0)))
            subdomains = defaultdict(set)
            word_counts = defaultdict(int, data.get("word_counts", {}))

            # convert subdomain lists to sets
            for subdomain, pages in data.get("subdomains", {}).items():
                subdomains[subdomain] = set(pages)
    except Exception as e:
        # start from scratch if there's an issue loading analytics
        unique_pages = set()
        word_counts = defaultdict(int)
        longest_page = ("", 0)
        subdomains = defaultdict(set)

def save_analytics():
    data = {
        "unique_pages": list(unique_pages),
        "longest_page": longest_page,
        "subdomains": {subdomain: list(pages) for subdomain, pages in subdomains.items()},
        "word_counts": dict(word_counts)
    }
    
    # write to a temp file and then resave to avoid corruption if program is interrupted during save
    temp_file = analytics + ".tmp"
    with open(temp_file, "w") as f:
        json.dump(data, f, indent=4)
    os.replace(temp_file, analytics)

def reset_analytics():
    global unique_pages, longest_page, subdomains, word_counts
    global pages_crawled_since_prev_save
    unique_pages = set()
    longest_page = ("", 0)
    subdomains = defaultdict(set)
    word_counts = defaultdict(int)
    pages_crawled_since_prev_save = 0

    # remove the persisted file so a crash mid-run can't accidentally resume the wiped state
    for path in (analytics, analytics + ".tmp"):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

def init_analytics(restart):
    if restart:
        reset_analytics()
    else:
        load_analytics()
    atexit.register(flush_on_exit)

def flush_on_exit():
    try:
        with analytics_lock:
            save_analytics()
            generate_report()
    except Exception:
        pass


def generate_report():
    top_words = sorted(word_counts.items(), key=lambda x: (-x[1], x[0]))[:50]

    sub_domain_report = sorted(f"{host} ({len(pages)} pages)" for host, pages in subdomains.items())
    with open("report.txt", "w", encoding="utf-8") as f:
        f.write(f"Number of unique pages: {len(unique_pages)}\n\n")
        f.write(f"Longest page: {longest_page[0]} ({longest_page[1]} words)\n\n")
        f.write("Top 50 most common words:\n")
        for word, count in top_words:
            f.write(f"  {word}: {count}\n")
        f.write("\nSubdomains under uci.edu:\n")
        for line in sub_domain_report:
            f.write(f"  {line}\n")
    

    
ALLOWED_DOMAIN_SUFFIXES = {"ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"}

# URL prefixes that require logins that we should skip crawling
LOGIN_WALLED_PREFIXES = {
    ("wiki.ics.uci.edu", "/doku.php"),
}

# Path that has consistent low info pages
LOW_INFO_PATH_SEGMENTS = ("/files/", "/page/", "/tag/", "/author/", "/category/", "/genealogy/")

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

# single out site header and navigation bars
BOILERPLATE_ATTR_PATTERNS = re.compile(
    r"(nav|menu|sidebar|footer|header|breadcrumb|skip-link|site-info|widget)",
    re.IGNORECASE,
)

def strip_boilerplate(soup):
    # remove non-text in-place so they don't pollute word counts.
    for tag in soup(["script", "style", "noscript", "nav", "header", "footer", "aside"]):
        tag.decompose()

    for tag in soup.find_all(attrs={"class": BOILERPLATE_ATTR_PATTERNS}):
        tag.decompose()
    for tag in soup.find_all(attrs={"id": BOILERPLATE_ATTR_PATTERNS}):
        tag.decompose()

    for tag in soup.find_all(attrs={"role": ["navigation", "banner", "contentinfo", "complementary"]}):
        tag.decompose()


# URLs embedded in plain-text pages (e.g. large .txt word lists)
PLAIN_TEXT_URL_RE = re.compile(r"\bhttps?://[^\s<>\]\)\"']+", re.IGNORECASE)


def words_from_page_text(page_text):
    """Return filtered lowercase word tokens (same rules as HTML extraction)."""
    page_text = page_text.replace("’", "'").replace("‘", "'")
    words = re.findall(r"\b[a-zA-Z]+(?:'[a-zA-Z]+)*\b", page_text)
    return [w.lower() for w in words if w.lower() not in STOP_WORDS]


def record_page_analytics(defragged_url, words_lowercase):
    """If this URL is new, update word counts, longest page, subdomains; may flush to disk."""
    global pages_crawled_since_prev_save, longest_page

    with analytics_lock:
        if defragged_url not in unique_pages:
            unique_pages.add(defragged_url)
            pages_crawled_since_prev_save += 1

            for word in words_lowercase:
                if len(word) > 1 and word not in STOP_WORDS:
                    word_counts[word] += 1

            if len(words_lowercase) > longest_page[1]:
                longest_page = (defragged_url, len(words_lowercase))

            host = urlparse(defragged_url).netloc
            if host.endswith(".uci.edu"):
                subdomains[host].add(defragged_url)

            if pages_crawled_since_prev_save >= save_interval:
                save_analytics()
                generate_report()
                pages_crawled_since_prev_save = 0


def extract_urls_from_plain_text(text, base_url):
    """Best-effort discovery of absolute http(s) URLs inside plain text."""
    seen = set()
    out = []
    for m in PLAIN_TEXT_URL_RE.finditer(text):
        u = m.group(0).rstrip(").,;:'\"")
        if u in seen:
            continue
        seen.add(u)
        absolute, _ = urldefrag(urljoin(base_url, u))
        out.append(absolute)
    return out


def _extract_plaintext(url, resp, content_size):
    """Handle text/plain and .txt bodies (not parsed as HTML)."""
    try:
        text = resp.raw_response.content.decode("utf-8", errors="replace")
    except Exception:
        return list()

    words_lowercase = words_from_page_text(text)
    if len(words_lowercase) < 50:
        return list()
    if content_size > 1_000_000 and len(words_lowercase) < 500:
        return list()

    if resp.raw_response and resp.raw_response.url:
        url_final = resp.raw_response.url
    else:
        url_final = url
    defragged_url, _ = urldefrag(url_final)

    record_page_analytics(defragged_url, words_lowercase)

    base = resp.raw_response.url if resp.raw_response else url
    return extract_urls_from_plain_text(text, base)


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


    # only parse if the response is valid and has content
    if resp.status != 200:
        return list()
    elif resp.raw_response is None:
        return list()
    elif not resp.raw_response.content:
        return list()

    content = resp.raw_response.content
    content_size = len(content)

    # Avoid really really large files. 10MB is a lot lot.
    if content_size > 10_000_000:
        return list()

    content_type = (resp.raw_response.headers.get("Content-Type") or "").lower()
    if resp.raw_response.url:
        path_for_type = urlparse(urldefrag(resp.raw_response.url)[0]).path.lower()
    else:
        path_for_type = urlparse(urldefrag(url)[0]).path.lower()

    # Plain text: often Content-Type: text/plain; also handle .txt when servers omit type.
    if ("text/plain" in content_type) or (
        path_for_type.endswith(".txt") and "text/html" not in content_type
    ):
        return _extract_plaintext(url, resp, content_size)

    # Non-HTML explicit types (PDF, images, etc.) — no analytics, no links.
    if content_type and "text/html" not in content_type:
        return list()

    # parse HTML (explicit text/html, or unknown/empty Content-Type — legacy behavior)
    try:
        soup = BeautifulSoup(content, "lxml")
    except Exception:
        return list()

    # extract links before stripping
    raw_hrefs = [a["href"] for a in soup.find_all("a", href=True)]

    strip_boilerplate(soup)

    page_text_content = soup.get_text(separator=" ")
    words_lowercase = words_from_page_text(page_text_content)

    # pages with less than 30 words can be considered low-information
    if len(words_lowercase) < 30:
        return list()

    # Avoid large pages with low information content.
    # 1 MB , assuming 30%$ of html is readable text, is 300KB of text. Assuming ASCII and 6 chars a word, thats 50k words approx.
    if content_size > 1_000_000 and len(words_lowercase) < 500: # less than 1% density of words per expected text size, pretty reasonable
        return list()

    # handle url redirects
    if resp.raw_response and resp.raw_response.url:
        url_final = resp.raw_response.url
    else:
        url_final = url

    defragged_url, _ = urldefrag(url_final)

    record_page_analytics(defragged_url, words_lowercase)

    # I initially wanted to use a set here to avoid duplicates, but frontier is already doing that check
    # extract links and normalize
    # make sure not empty and not a link that we don't wanna crawl
    extracted_links = []
    for href in raw_hrefs:
        if not href:
            continue
        if href.startswith(("mailto:", "javascript:", "tel:", "ftp:")):
            continue
        absolute, _ = urldefrag(urljoin(resp.raw_response.url, href))
        extracted_links.append(absolute)

    return extracted_links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        if not isinstance(url, str):
            return False
        url, _ = urldefrag(url)
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)

        # Trap Prevention
        if query_params:
            # If there are query parameters, don't crawl to avoid common traps. Calendar traps are a notable one
            return False

        if parsed.path.count('/') > 8:
            # If the path has more than 8 slashes, it's probably not a valid url
            return False

        # Avoid calendar traps if date in path in format YYYY-MM-DD or YYYY-MM at the end.
        # Only check at end because news articles sometimes have dates at start of path or middle.
        # Allow an optional trailing slash.
        if re.search(r"/\d{4}-\d{2}-\d{2}/?$", parsed.path) or re.search(r"/\d{4}-\d{2}/?$", parsed.path):
            # If the path contains a date, it's probably not a valid url or low value page
            # For avoiding calendar traps if date not in query params
            return False

        path_parts = [p for p in parsed.path.split('/') if p] # don't include empty parts
        if len(path_parts) != len(set(path_parts)):
            # Avoid repeating paths
            return False
        
        # only crawl urls in the domain mentioned on canvas
        host = (parsed.hostname or "").lower().rstrip(".")
        if not any(host == suffix or host.endswith("." + suffix)
            for suffix in ALLOWED_DOMAIN_SUFFIXES):
            # check if the host is in the allowed domain suffixes or a complete match
            return False

        # skip login pages
        #for blocked_host, blocked_prefix in LOGIN_WALLED_PREFIXES:
        #    if host == blocked_host and parsed.path.startswith(blocked_prefix):
        #        return False

        #path_lower = parsed.path.lower()
        #for segment in LOW_INFO_PATH_SEGMENTS:
        #    if segment in path_lower:
        #        return False

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
