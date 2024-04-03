import threading

from bs4 import BeautifulSoup
import requests
import time
from urllib.parse import urlparse, urljoin
import urllib.robotparser as urlrobot
from queue import PriorityQueue
import json
import os
from langdetect import detect
import langid
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor

# Initialize ThreadPoolExecutor with a suitable number of threads
executor = ThreadPoolExecutor(max_workers=10)
rate_limit = threading.Semaphore(8)  # num of threads that can access the resource at the same time
# no point in semaphore num being greater than max workers

# common seeds
common_1 = 'https://climate.nasa.gov'
common_2 = 'http://en.wikipedia.org/wiki/Climate_change'

# individual
wiki_1 = 'http://en.wikipedia.org/wiki/Effects_of_climate_change_on_humans'
wiki_2 = 'http://en.wikipedia.org/wiki/Economic_impacts_of_climate_change'

# key terms
terms_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Code/terms.txt'
terms = []

# initialize queue
frontier = PriorityQueue()
queue_items = []

queue_lock = threading.Lock()
wave_lock = threading.Lock()
outlinks_lock = threading.Lock()
inlinks_lock = threading.Lock()

# json files
outlinks_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/outlinks.json'
if os.path.exists(outlinks_file):
    with open(outlinks_file, 'r') as json_file:
        outlinks = json.load(json_file)
else:
    outlinks = {}

inlinks_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/inlinks.json'
if os.path.exists(inlinks_file):
    with open(inlinks_file, 'r') as json_file:
        inlinks = json.load(json_file)
else:
    inlinks = {}

frontier_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/frontier.json'
if os.path.exists(frontier_file):
    with open(frontier_file, 'r') as json_file:
        frontier_data = json.load(json_file)
    for priority, data in frontier_data:
        frontier.put((priority, data))
        queue_items.append((priority, data))
else:
    frontier.put((1, common_1))
    frontier.put((2, common_2))
    frontier.put((3, wiki_1))
    frontier.put((4, wiki_2))
    queue_items = [(1, common_1), (2, common_2), (3, wiki_1), (4, wiki_2)]

wave_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/wave.json'
if os.path.exists(wave_file):
    with open(wave_file, 'r') as json_file:
        link_wave = json.load(json_file)
else:
    # initialize dict, link -> wave number
    link_wave = {common_1: 0, common_2: 0, wiki_1: 0, wiki_2: 0}

crawled_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/crawled.json'
if os.path.exists(crawled_file):
    with open(crawled_file, 'r') as json_file:
        crawled_items = json.load(json_file)
else:
    crawled_items = []


# get url outlinks and add to dict
def get_outlinks(url):
    with outlinks_lock:
        if url in outlinks:
            return
        outlinks_list = []
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        for link in soup.find_all('a', href=True):
            outlink = canonicalize_link(url, link['href'])
            if outlink != '://' and outlink.startswith('http') and outlink not in outlinks_list:
                outlinks_list.append(outlink)
        outlinks[url] = outlinks_list
        with open(outlinks_file, 'w') as json_file:
            json.dump(outlinks, json_file, indent=2)


# get url inlinks from outlinks dict and add to inlinks dict/json
def get_inlinks():
    with inlinks_lock:
        for key, values in outlinks.items():
            for outlink in values:
                if outlink not in inlinks:
                    inlinks[outlink] = []
                if key not in inlinks[outlink]:
                    inlinks[outlink].append(key)

        with open(inlinks_file, 'w') as json_file:
            json.dump(inlinks, json_file, indent=2)


# canonicalize link
def canonicalize_link(base_link, link):
    if link.startswith('/') or link.startswith('../'):
        link = urljoin(base_link, link)
    if link.endswith('/'):
        link = link[:-1]
    split = link.split('/')
    parts = [segment for segment in split if segment]
    link = '/'.join(parts)
    link = link.split('#')[0]
    parsed_url = urlparse(link)
    scheme = parsed_url.scheme.lower()
    host = parsed_url.netloc.lower()
    host = host.split(':')[0]
    parsed_url = parsed_url.path
    updated = scheme + ':/' + host + parsed_url
    return updated


# fetch robots.txt before crawling and follow policies
def get_robotstxt(url):
    try:
        rp = urlrobot.RobotFileParser()
        rp.set_url(url + '/robots.txt')
        rp.read()
        crawl_delay = rp.crawl_delay('*')
        if crawl_delay is not None:
            time.sleep(crawl_delay)
        else:
            # default delay = 1 second
            time.sleep(1)
    except Exception as e:
        print(url, 'failed robot', e)
        pass


# read through terms.txt
with open(terms_file, 'r') as file:
    for line in file:
        terms.append(line.strip())


# get similarity score of doc and terms.txt
def similarity_score(url):
    response = requests.get(url)
    if response.headers.get('content-type') and 'html' in response.headers.get('content-type'):
        soup = BeautifulSoup(response.text, 'html.parser')
    try:
        score = 0
        anchor_text = soup.find_all('a')
        for anchor in anchor_text:
            for word in anchor.get_text().split():
                if word.lower() in terms:
                    score += 5
    except RequestException as e:
        score = 0
    if soup.title:
        title = soup.title.get_text().strip()
        for word in title.split():
            if word.lower() in terms:
                score += 3
    for word in terms:
        if word in url:
            score += 1
    score += len(inlinks.get(url, []))
    if '.gov' in url or 'edu' in url:
        score += 35
    score = score * 3
    return score


excluding = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
             '.fcgi', '/retrieve', '.php', '.ashx', '.xml']


# fetch metadata and check content type and language
def check_metadata(url):
    get_robotstxt(url)
    response = requests.get(url)
    # check that content type is html
    if response.headers.get('content-type') and 'html' in response.headers.get('content-type'):
        soup = BeautifulSoup(response.text, 'html.parser')
        # check text language is english
        lang = detect(soup.get_text())
        lang2, confidence = langid.classify(soup.get_text())
        if lang == 'en' or lang2 == 'en':
            get_outlinks(url)
            get_inlinks()
            # find all outlinks
            links = soup.find_all('a', href=True)
            print('number of links:', len(links))
            for num, link in enumerate(links):
                outlink = link.get('href')
                outlink = canonicalize_link(url, outlink)
                # only add valid links to frontier
                print(num, 'checking', outlink)
                if any(outlink.endswith(ending) for ending in excluding):
                    print('not html')
                    continue
                get_robotstxt(outlink)
                try:
                    resp = requests.get(outlink)
                    # print('getting response')
                    if resp.status_code == 200:
                        # score and add to frontier queue
                        with wave_lock:
                            get_outlinks(outlink)
                            score = similarity_score(outlink)
                            if outlink not in link_wave:
                                link_wave[outlink] = link_wave[url] + 1
                            score -= link_wave[outlink] * 10
                            print("in queue =", 50000 - score, outlink)
                            queue_items.append((50000 - score, outlink))
                            frontier.put((50000 - score, outlink))
                except RequestException as e:
                    print(outlink, e, 'getting outlink failed')
                if num % 100 == 0:
                    get_inlinks()
                    with queue_lock:
                        with open(frontier_file, 'w') as json_file:
                            json.dump(queue_items, json_file, indent=2)
                        with open(wave_file, 'w') as json_file:
                            json.dump(link_wave, json_file, indent=2)
                        print('updated frontier and wave num file')


# process the document and store HTTP
def crawl_parse(url):
    doc_file = f"C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Parsed_Docs/docs_{link_wave[url]}.txt"
    get_robotstxt(url)
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    if soup.title:
        title = soup.title.get_text().strip()
    else:
        title = ''
    content = ''
    for p_text in soup.find_all('p'):
        text = p_text.get_text().strip()
        content += text + '\n'
    # write to file
    formatted = f'<DOC>\n<DOCNO>{url}</DOCNO>\n<HEAD>{title}</HEAD>\n<TEXT>\n{content}</TEXT>\n</DOC>\n'
    with open(doc_file, 'a', encoding='utf-8') as file:
        file.write(formatted)


def crawl(url):
    with rate_limit:
        print("next on frontier, score =", score, ", url:", url)
        crawl_parse(url)
        print('doc processed')
        check_metadata(url)
        print('got outlinks of doc')


# # testing
# print('HTTP://www.Example.com/SomeFile.html', canonicalize_link('', 'HTTP://www.Example.com/SomeFile.html'))
# print('http://www.example.com:80', canonicalize_link('', 'http://www.example.com:80'))
# print('http://www.example.com/a/b.html, ../c.html', canonicalize_link('http://www.example.com/a/b.html', '../c.html'))
# print('http://www.example.com/a.html#anything', canonicalize_link('', 'http://www.example.com/a.html#anything'))
# print('http://www.example.com//a.html', canonicalize_link('', ' http://www.example.com//a.html'))

# go through frontier
while len(crawled_items) < 11000:
    score, url = frontier.get()
    print('doc crawl #', len(crawled_items), url)
    if url not in crawled_items:
        executor.submit(crawl, url)
        crawled_items.append(url)
        print(len(crawled_items), 'docs crawled', url)
        with open(crawled_file, 'w') as json_file:
            json.dump(crawled_items, json_file, indent=2)
    else:
        print(url, 'already crawled')

print('finished crawling 11000 docs')
