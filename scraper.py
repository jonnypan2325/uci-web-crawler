import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
import os
import json
from collections import defaultdict

analytics = "analytics.json"
save_interval = 50
pages_crawled_since_prev_save = 0


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
    
    # parse the content with beautifulsoup]
    try: 
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
    except Exception as e:
        return list()
    
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
        absolute = urljoin(resp.raw_response.url, href)
        extracted_links.append(absolute)

    return extracted_links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # only crawl urls in the domain mentioned on canvas
        if not re.match(r"^(.*\.)?(ics|cs|stat|informatics)\.uci\.edu$", parsed.netloc.lower()):
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
