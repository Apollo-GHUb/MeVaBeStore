# File: app.py
from flask import Flask, render_template
import polars as pl

app = Flask(__name__)

# Tải dữ liệu 1 lần duy nhất khi server khởi động
items_df = pl.read_parquet('items.parquet')
# Loại bỏ các dòng bị lỗi (null) ở cột category_l3 hoặc category để hiển thị cho đẹp
items_clean = items_df.drop_nulls(subset=["category_l3"])


@app.route('/')
def home():
    # Lấy ngẫu nhiên/hoặc top 16 sản phẩm để hiển thị lên Trang chủ
    sample_products = items_clean.head(16).to_dicts()

    # Trả về trang giao diện HTML và truyền dữ liệu sản phẩm vào
    return render_template('index.html', products=sample_products)


if __name__ == '__main__':
    # Chạy server ở port 5000
    app.run(debug=True, port=5000)
