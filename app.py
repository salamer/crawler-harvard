import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify
import urllib.parse
from urllib.parse import urljoin
from functools import reduce
import redis

redis_client = redis.Redis(
  host='hello-ytox-efcf-405444.leapcell.cloud',
  port=6379,
  password='Ae00000gjSXe+6xducNs4Hr4LX7gE07L9ORCCMTyyNaXqAWFnNF+pjblUQdYX7kc32B6hRy',
  ssl=True
)

app = Flask(__name__)

baseUrl = 'https://seas.harvard.edu/computer-science'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch, br',
}

def parse_url(url):
    if url is None:
        return None
    base = urllib.parse.urlparse(baseUrl)
    domain = f'{base.scheme}://{base.netloc}'
    if url.startswith('/'):
        return urljoin(domain, url)
    if url.startswith('http'):
        u = urllib.parse.urlparse(url)
        if u.netloc == base.netloc:
            return url
    return None

def has_crawl(url):
    r = redis_client.setnx('has_crawlqqqqqqqxxqqqxxx' + url, 1)
    return r == False


def crawl(url: str):
    session = requests.Session()
    response = session.get(url, headers=headers)
    if response.status_code != 200:
        return jsonify({
            'error': 'Failed to fetch the page'
        })
    r = redis_client.incr('total_pages')
    print(f'Total pages: {r}')
    soup = BeautifulSoup(response.content, 'html.parser')
    hrefs = soup.find_all('a')
    links = [parse_url(href.get('href')) for href in hrefs]
    links = list(filter(lambda x: x is not None, links))

    links = list(filter(lambda x: not has_crawl(x), links))

    for link in links:
        crawl(link)
        requests.post('https://crawler-harvard.leapcell.cloud/crawl-page', json={'url': link})
    return links

@app.route('/crawl-page')
def crawl_page():
    links = crawl(baseUrl)

    return jsonify({
        'links': links
    })

if __name__ == '__main__':
    app.run(debug=True)