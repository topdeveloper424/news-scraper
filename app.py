from flask import Flask
import scraper
from flask import jsonify

app = Flask(__name__)

@app.route('/')
def hello():
    return "Hello, World!"

@app.route('/wsj', methods=['GET'])
def get_wsj():
    return jsonify(scraper.scrape_wsj())

@app.route('/wsp', methods=['GET'])
def get_wsp():
    return jsonify(scraper.scrape_wsp())

if __name__ == "__main__":
    app.run(host='0.0.0.0')