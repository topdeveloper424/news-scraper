from flask import Flask, request, render_template
from flask import jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
import requests
from bs4 import BeautifulSoup
import json
from futures3.thread import ThreadPoolExecutor
import time

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///newsscraper.db'
db = SQLAlchemy(app)

class WEBSITES:
    WSJ = "wsj"
    WSP = "wsp"

class NewsData(db.Model):
    __tablename__ = 'news_data'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    website = db.Column(db.String(10))
    headline = db.Column(db.Text)
    url = db.Column(db.Text)
    summary = db.Column(db.Text, nullable=True)
    image_link = db.Column(db.Text, nullable=True)
    author = db.Column(db.String(255), nullable=True)
    pub_datetime = db.Column(db.DateTime)
    def to_dict(self):
        # Serialize the Product object to a dictionary
        return {
            'id': self.id,
            'website': self.website,
            'headline': self.headline,
            'url': self.url,
            'summary': self.summary,
            'image_link': self.image_link,
            'author': self.author,
            'pub_datetime': self.pub_datetime
        }    

with app.app_context():
    db.create_all()

def cron_func():
    print("scraping.......")
    scrape_wsj()
    scrape_wsp()

sched = BackgroundScheduler(daemon=True)
sched.add_job(cron_func,'interval',minutes=60)
#sched.add_job(cron_func,'interval',minutes=60, next_run_time=datetime.datetime.now())
sched.start()

@app.template_filter('datetime_format')
def datetime_format(value, format='%Y-%m-%d %H:%M:%S'):
    return value.strftime(format)

@app.template_filter('max_filter')
def max_filter(a, b):
    return max(a, b)

@app.template_filter('min_filter')
def min_filter(a, b):
    return min(a, b)

@app.template_filter('min_filter')
def min_filter(a, b):
    return min(a, b)

@app.route('/')
def hello():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 9))
    offset = (page - 1) * per_page
    total_count = db.session.query(func.count(NewsData.id)).scalar()
    products = NewsData.query.order_by(NewsData.pub_datetime.desc()).offset(offset).limit(per_page).all()
    return render_template('home.html', items=products, total_items=total_count, current_page=page, items_per_page=per_page)

@app.route('/data', methods=['GET'])
def get_data():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 9))
    offset = (page - 1) * per_page
    with app.app_context():
        total_count = db.session.query(func.count(NewsData.id)).scalar()
        products = NewsData.query.order_by(NewsData.pub_datetime.desc()).offset(offset).limit(per_page).all()

        response = {
            'page': page,
            'per_page': per_page,
            'total_count': total_count,
            'newsdata': [product.to_dict() for product in products]  # Serialize the products to a JSON-serializable format
        }

        return jsonify(response)

@app.route('/wsj', methods=['GET'])
def get_wsj():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    offset = (page - 1) * per_page
    with app.app_context():
        total_count = db.session.query(func.count(NewsData.id)).filter(NewsData.website == WEBSITES.WSJ).scalar()
        products = NewsData.query.filter(NewsData.website == WEBSITES.WSJ).order_by(NewsData.pub_datetime.desc()).offset(offset).limit(per_page).all()

        response = {
            'page': page,
            'per_page': per_page,
            'total_count': total_count,
            'newsdata': [product.to_dict() for product in products]  # Serialize the products to a JSON-serializable format
        }

        return jsonify(response)

@app.route('/wsp', methods=['GET'])
def get_wsp():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    offset = (page - 1) * per_page
    with app.app_context():
        total_count = db.session.query(func.count(NewsData.id)).filter(NewsData.website == WEBSITES.WSP).scalar()
        products = NewsData.query.filter(NewsData.website == WEBSITES.WSP).order_by(NewsData.pub_datetime.desc()).offset(offset).limit(per_page).all()

        response = {
            'page': page,
            'per_page': per_page,
            'total_count': total_count,
            'newsdata': [product.to_dict() for product in products]  # Serialize the products to a JSON-serializable format
        }

        return jsonify(response)

def get_latest_record(website):
    latest_record = None
    with app.app_context():
        latest_record = db.session.query(NewsData).filter(NewsData.website == website).order_by(NewsData.pub_datetime.desc()).first()
        print(latest_record)
    return latest_record

def scrape_wsj():
    latest_date = get_latest_record(WEBSITES.WSJ)
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36", "Scheme":"https"}
    url = "https://www.wsj.com/search?query={}&mod=searchresults_viewallresults&startDate={}&endDate={}&sort=date-desc&page="
    start_date = "2023/04/01".replace("/","%2F")
    today_date = datetime.datetime.now().date().strftime("%Y/%m/%d")
    today_date = today_date.replace("/","%2F")
    encoded_url = url.format("",start_date, today_date)
    page_num = 1
    break_flag = False
    with app.app_context():
        while True:
            page_url = encoded_url + str(page_num)
            print(page_url)
            counter = 0
            articles = []
            while counter < 6:
                response = requests.get(page_url,headers=headers)
                soup = BeautifulSoup(response.content, 'html.parser')
                container = soup.find("main",{"id":"main"})
                articles = container.find_all("article")
                if len(articles) > 0:
                    break
                else:
                    time.sleep(1)
                counter += 1

            ids = []
            for article in articles:
                data_id = article["data-id"]
                ids.append(data_id)

            if len(ids) == 0:
                break
            print(ids)
            threads = min(10, len(ids))
            with ThreadPoolExecutor(max_workers=threads) as executor:
                grab_results = executor.map(scrape_wsj_page, ids)
                for result in grab_results:
                    if result:
                        if latest_date is not None and latest_date.pub_datetime >= result["datetime"]:
                            break_flag = True
                            break
                        else:
                            print(result)
                            news_data = NewsData(website=WEBSITES.WSJ, headline=result["headline"], url=result["url"] , summary=result["summary"], image_link=result["image_link"], author=result["author"], pub_datetime=result["datetime"])
                            db.session.add(news_data)
                            db.session.flush()

            db.session.commit()
            if break_flag:
                break

            page_num += 1

def scrape_wsj_page(data_id):
    news = None
    try:
        headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36", "Scheme":"https"}
        article_api = "https://www.wsj.com/search?id="+data_id+"&type=article%7Ccapi"
        article_json = requests.get(article_api, headers=headers).json()
        article_data = article_json["data"]
        dt = datetime.datetime.fromtimestamp(article_data["timestampCreatedAt"] / 1000)
        tags = []
        try:
            tags = article_data["tags"]
        except Exception:
            pass
        if "EXCLUSIVE" in tags:
            news = {
                "headline": article_data["headline"],
                "url" : article_data["url"],
                "summary" : article_data["summary"],
                "image_link": article_data["arthurV2Image"]["location"],
                "author": article_data["byline"],
                "datetime": dt
            }
    except Exception:
        pass
    return news


def scrape_wsp():
    latest_date = get_latest_record(WEBSITES.WSP)
    start_date = datetime.datetime(2023, 4, 1)
    start_timestamp = int(start_date.timestamp() * 1000)
    today_date = datetime.datetime.now()
    today_timestamp = int(today_date.timestamp() * 1000)

    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36", "Scheme":"https"}
    url = "https://o19n5yy9r3-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.13.1)%3B%20Browser%3B%20JS%20Helper%20(3.9.0)%3B%20react%20(17.0.2)%3B%20react-instantsearch%20(6.29.0)"
    headers["X-Algolia-Api-Key"] = "b867428aeea9d17f07f77f54676d0317"
    headers["X-Algolia-Application-Id"] = "O19N5YY9R3"
    page_num = 0
    break_flag = False
    with app.app_context():
        while True:
            form_data = {"requests":[{"indexName":"crawler_wapocrawl_sortby_publish_date","params":"analytics=true&clickAnalytics=true&distinct=true&facetFilters=%5B%5B%5D%2C%5B%5D%5D&facets=%5B%5D&filters=publish_date_timestamp%3A"+str(start_timestamp)+"%20TO%20"+str(today_timestamp)+"&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=50&page="+str(page_num)+"&query=&tagFilters="}]}
            response = requests.post(url, data= json.dumps(form_data), headers=headers).json()
            articles = response["results"][0]["hits"]
            if len(articles) == 0:
                break
            threads = min(10, len(articles))
            with ThreadPoolExecutor(max_workers=threads) as executor:
                grab_results = executor.map(scrape_wsp_page, articles)
                grab_results = [x for x in grab_results if x is not None]
                if len(grab_results) > 1:
                    grab_results = sorted(grab_results, key=lambda x: x['publish_date_timestamp'], reverse=True)
                for article in grab_results:
                    if article:
                        try:
                            content = "<div>"+article["content"]+"</div>"
                            content_html = BeautifulSoup(content, 'html.parser')
                            content_text = content_html.text
                            if len(content_text) > 500:
                                content_text = content_text[:500] + "..."
                            dt = datetime.datetime.fromtimestamp(article["publish_date_timestamp"] / 1000)
                            
                            if latest_date is not None and latest_date.pub_datetime >= dt:
                                break_flag = True
                                break

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
                                "datetime": dt
                            }
                            print(news)
                            news_data = NewsData(website=WEBSITES.WSP, headline=news["headline"], url=news["url"] , summary=news["summary"], image_link=news["image_link"], author=news["author"], pub_datetime=news["datetime"])
                            db.session.add(news_data)
                            db.session.flush()
                        except TimeoutError:
                            pass

            db.session.commit()
            if break_flag:
                break

            page_num += 1


def scrape_wsp_page(article):
    try:
        headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36", "Scheme":"https"}
        response = requests.get(article["url"], headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        wp_exclusive_label = soup.find("span",{"class":"content-box", "data-qa":"wp-exclusive"})
        if wp_exclusive_label:
            return article
    except Exception:
        pass
    return None


if __name__ == "__main__":
    app.run(host='0.0.0.0')