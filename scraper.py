import requests
from bs4 import BeautifulSoup
import datetime
import json
from futures3.thread import ThreadPoolExecutor


def scrape_wsj():
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36", "Scheme":"https"}
    url = "https://www.wsj.com/search?query=WSJ%20NEWS%20EXCLUSIVE&mod=searchresults_viewallresults&sort=date-desc&page="
    page_num = 1
    news_data = []
    while page_num < 2:
        page_url = url + str(page_num)
        response = requests.get(page_url,headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        container = soup.find("main",{"id":"main"})
        articles = container.find_all("article")
        ids = []
        for article in articles:
            data_id = article["data-id"]
            ids.append(data_id)
        threads = min(10, len(ids))
        with ThreadPoolExecutor(max_workers=threads) as executor:
            grab_results = executor.map(scrape_page, ids)
            news_data += grab_results

        page_num += 1
    news_data = list(filter(lambda x: x is not None, news_data))

    return news_data

def scrape_page(data_id):
    news = None
    try:
        headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36", "Scheme":"https"}
        article_api = "https://www.wsj.com/search?id="+data_id+"&type=article%7Ccapi"
        article_json = requests.get(article_api, headers=headers).json()
        article_data = article_json["data"]
        dt = datetime.datetime.fromtimestamp(article_data["timestampCreatedAt"] / 1000)
        datetime_text = dt.strftime('%Y-%m-%d %H:%M:%S')

        news = {
            "headline": article_data["headline"],
            "url" : article_data["url"],
            "summary" : article_data["summary"],
            "image_link": article_data["arthurV2Image"]["location"],
            "author": article_data["byline"],
            "datetime": datetime_text
        }
    except Exception:
        pass
    return news


def scrape_wsp():
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36", "Scheme":"https"}
    url = "https://o19n5yy9r3-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.13.1)%3B%20Browser%3B%20JS%20Helper%20(3.9.0)%3B%20react%20(17.0.2)%3B%20react-instantsearch%20(6.29.0)"
    headers["X-Algolia-Api-Key"] = "b867428aeea9d17f07f77f54676d0317"
    headers["X-Algolia-Application-Id"] = "O19N5YY9R3"
    page_num = 0
    news_data = []
    while page_num < 2:
        form_data = {"requests":[{"indexName":"crawler_wapocrawl_sortby_publish_date","params":"analytics=true&clickAnalytics=true&distinct=true&facetFilters=%5B%5B%5D%2C%5B%5D%5D&facets=%5B%5D&filters=&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=50&page="+str(page_num)+"&query=exclusive%20news&tagFilters="}]}
        response = requests.post(url, data= json.dumps(form_data), headers=headers).json()
        articles = response["results"][0]["hits"]

        for article in articles:

            try:
                content = "<div>"+article["content"]+"</div>"
                content_html = BeautifulSoup(content, 'html.parser')
                content_text = content_html.text
                if len(content_text) > 500:
                    content_text = content_text[:500] + "..."
                datetime_text = ""
                try:
                    dt = datetime.datetime.fromtimestamp(article["publish_date_timestamp"] / 1000)
                    datetime_text = dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    pass
                
                author = ""
                try:
                    author = article["author"]
                    author = author.strip()
                except Exception:
                    pass

                news = {
                    "headline": article["title"],
                    "url" : article["url"],
                    "summary" : content_text,
                    "image_link": article["thumbnail"],
                    "author": author,
                    "datetime": datetime_text
                }
                print(news)
                news_data.append(news)
            except Exception:
                pass

        page_num += 1
    return news_data
