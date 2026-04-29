import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup

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
