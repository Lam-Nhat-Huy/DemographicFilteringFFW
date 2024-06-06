import pandas as pd
from flask import Flask, jsonify
from gevent.pywsgi import WSGIServer
import json
import warnings
import  mysql.connector
import logging

# Khởi chạy falsk
app = Flask(__name__)


warnings.simplefilter('ignore')
logging.basicConfig(level=logging.DEBUG)
database = mysql.connector.connect(host='localhost', user='root', password='mysql', database='ffw')

# hoặc dùng cách này để get data
df2 = pd.read_sql('SELECT p.product_id, p.product_name, p.product_title, p.product_price, p.product_sale, p.product_img, p.product_quantily, p.category_id, p.created_at, AVG(r.review_rating) AS vote_average, COUNT(r.review_rating) AS vote_count FROM product p JOIN reviews r ON p.product_id = r.product_id GROUP BY p.product_id ORDER BY vote_average DESC;', database)


@app.route('/hello', methods=['GET'])
def hello_world():
    return 'Hello World!'

# Lấy tất cả dữ liệu trong database
@app.route('/api/data/movies', methods=['GET'])
def get_df2_data():
    try:
        json_str = df2.to_json(orient='records')
        json_obj = json.loads(json_str)
        return jsonify(json_obj)
    except Exception as e:
        logging.error(f"Có một lõi đã xảy ra: {e}")
        return jsonify({"error": "Có một lỗi đã xảy ra"}), 500

@app.route('/api/data/demographic', methods=['GET'])
def getFilmByDemographicFiltering():
    try:
        sql = "SELECT p.product_id, p.product_name, p.product_title, p.product_price, p.product_sale, p.product_img, p.product_quantily, p.category_id, p.created_at, AVG(r.review_rating) AS vote_average, COUNT(r.review_rating) AS vote_count FROM product p JOIN reviews r ON p.product_id = r.product_id GROUP BY p.product_id ORDER BY vote_average DESC;"
        C = df2['vote_average'].mean()
        m = df2['vote_count'].quantile(0.9)
        # Thực hiện truy vấn sql để lấy tất cả dữ liệu
        df_all_movies = pd.read_sql(sql , database)
        def weighted_rating(x):
            R = x['vote_average']
            v = x['vote_count']
            try:
                score = (v / (v + m) * R) + (m / (m + v) * C)
            except ZeroDivisionError:
                logging.error(f"Đã xảy ra lỗi phân chia bằng 0 đối với bản ghi: {x}")
                score = 0
            return score
        # Áp dụng hàm weighted_rating và sắp xếp dữ liệu
        df_all_movies['score'] = df_all_movies.apply(weighted_rating, axis=1)
        df_all_movies = df_all_movies.sort_values('score', ascending=False)
        # Chuyển đổi dataframe thành json và trả về
        json_str = df_all_movies[['product_id', 'product_name', 'product_title', 'product_price', 'product_sale', 'product_img', 'product_quantily', 'category_id', 'created_at', 'vote_average', 'vote_count', 'score']].to_json(orient='records')
        json_obj = json.loads(json_str)
        return jsonify(json_obj)
    except Exception as e:
        logging.error(f"Đã có lỗi xảy ra: {e}")
        return jsonify({"error": "Đã có lỗi xảy ra"}), 500



if __name__ == "__main__":
    http_server = WSGIServer(("127.0.0.1", 4000), app)
    http_server.serve_forever()