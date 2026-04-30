import re
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from bs4 import BeautifulSoup
import os
import json
import atexit
from collections import defaultdict

analytics = "analytics.json"
save_interval = 50
pages_crawled_since_prev_save = 0

# stop words from : https://gist.github.com/sebleier/554280

STOP_WORDS = set(["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", 
                  "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", 
                  "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", 
                  "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", 
                  "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", 
                  "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", 
                  "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", 
                  "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", 
                  "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", 
                  "very", "s", "t", "can", "will", "just", "don", "should", "now"])

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

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

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
    
    # only parse if the content type is html, not pdfs or images or other files
    content_type = resp.raw_response.headers.get("Content-Type", "")
    if content_type and "text/html" not in content_type:
        return list()

    # Avoid really really large files. 10MB is a lot lot. 
    content_size = len(resp.raw_response.content)
    if content_size > 10_000_000:
        return list()

    # parse the content with beautifulsoup]
    try: 
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
    except Exception as e:
        return list()
    
    # we can track only the alphabetic words, as the analytics doesn't care about numbers or special characters
    page_text_content = soup.get_text(separator=" ")
    page_text_content = page_text_content.replace("’", "'").replace("‘", "'") # normalize by replacing apostrophes with single quotes
        # tested and actually doesn't change word count for the dataset tested. Will leave for now.
        
    # regex to match words with apostrophes(not at front or end)
    words = re.findall(r"\b[a-zA-Z]+(?:'[a-zA-Z]+)*\b", page_text_content)
    words_lowercase = [word.lower() for word in words if word.lower() not in STOP_WORDS] # only include words not in stop words set
    #print("Number of words:", len(words_lowercase)) debugging 
    #print("Words:", words_lowercase) debugging

    # pages with less than 50 words can be considered low-information
    if len(words_lowercase) < 50:
        return list()

    # Avoid large pages with low information content.
    # 1 MB , assuming 30%$ of html is readable text, is 300KB of text. Assuming ASCII and 6 chars a word, thats 50k words approx.
    if content_size > 1_000_000 and len(words_lowercase) < 500: # less than 1% density of words per expected text size, pretty reasonable
        return list() 
    
    global pages_crawled_since_prev_save, longest_page


    # handle url redirects
    if resp.raw_response and resp.raw_response.url:
        url_final = resp.raw_response.url 
    else:
        url_final = url

    defragged_url, _ = urldefrag(url_final)

    if defragged_url not in unique_pages:
        unique_pages.add(defragged_url)
        pages_crawled_since_prev_save += 1

        # only store tokens that are meaningful
        for word in words_lowercase:
            if len(word) > 1 and word not in STOP_WORDS:
                word_counts[word] += 1
        
        # update longest page
        if len(words_lowercase) > longest_page[1]:
            longest_page = (defragged_url, len(words_lowercase))

        
        host = urlparse(defragged_url).netloc
        if host.endswith(".uci.edu"):
            subdomains[host].add(defragged_url)
        
        # flush to disk
        if pages_crawled_since_prev_save >= save_interval:
            save_analytics()
            generate_report()
            pages_crawled_since_prev_save = 0
    
    # I initially wanted to use a set here to avoid duplicates, but frontier is already doing that check
    # extract links and normalize
    # make sure not empty and not a link that we don't wanna crawl
    extracted_links = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
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
