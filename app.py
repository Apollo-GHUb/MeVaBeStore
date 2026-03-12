import os
from flask import Flask, render_template, request, url_for
import polars as pl
from models import recommend_similar_items, recommend_by_category_score

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, template_folder=os.path.join(BASE_DIR, 'templates'))

# TẢI VÀ LÀM SẠCH DỮ LIỆU TỰ ĐỘNG
try:
    items_df = pl.read_parquet(os.path.join(BASE_DIR, 'items.parquet'))

    # BƯỚC QUYẾT ĐỊNH: Xóa sạch mọi khoảng trắng ẩn ở đầu/cuối của dữ liệu
    items_df = items_df.with_columns([
        pl.col("category_l1").str.strip_chars(),
        pl.col("category_l2").str.strip_chars(),
        pl.col("category_l3").str.strip_chars()
    ])

    trans_path = os.path.join(BASE_DIR, 'transactions_mini.parquet')
    if not os.path.exists(trans_path):
        trans_path = os.path.join(BASE_DIR, 'transactions-2025-12.parquet')

    transactions_df = pl.read_parquet(trans_path).drop_nulls(
        subset=["customer_id", "item_id"])
    items_clean = items_df.drop_nulls(subset=["category_l3"])

    # Chỉ lấy những danh mục L1 thực sự có chứa sản phẩm
    nav_categories = (
        items_clean.group_by("category_l1")
        .agg(pl.len().alias("count"))
        .filter(pl.col("count") > 0)  # Xóa ngay những danh mục rỗng
        .sort("count", descending=True)
        .head(6)
        .get_column("category_l1")
        .to_list()
    )
except Exception as e:
    print(f"Lỗi tải dữ liệu: {e}")


@app.route('/')
def home():
    cat_filter = request.args.get('category')
    search_query = request.args.get('search')

    # Lấy số trang hiện tại từ URL (Mặc định là trang 1)
    page = request.args.get('page', 1, type=int)
    ITEMS_PER_PAGE = 40  # Mỗi trang hiển thị 40 sản phẩm

    filtered = items_clean

    # 1. Lọc theo danh mục
    if cat_filter and cat_filter != "all":
        cat_filtered = filtered.filter(pl.col("category_l1") == cat_filter)
        if cat_filtered.height == 0:
            cat_filtered = filtered.filter(pl.col("category_l2") == cat_filter)
        filtered = cat_filtered

    # 2. Lọc theo từ khóa tìm kiếm
    if search_query:
        search_lower = search_query.lower()
        filtered = filtered.filter(
            pl.col("category_l3").str.to_lowercase().str.contains(search_lower)
        )

    # 3. THUẬT TOÁN PHÂN TRANG (PAGINATION)
    total_items = filtered.height
    # Tính tổng số trang cần thiết
    total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE

    # Nếu trang yêu cầu vượt quá tổng số trang, đưa về trang cuối
    if page > total_pages and total_pages > 0:
        page = total_pages

    # Cắt dữ liệu (Chỉ lấy đúng 40 sản phẩm của trang hiện tại)
    offset = (page - 1) * ITEMS_PER_PAGE
    if total_items > 0:
        sample_products = filtered.slice(offset, ITEMS_PER_PAGE).to_dicts()
    else:
        sample_products = []

    return render_template('index.html',
                           products=sample_products,
                           categories=nav_categories,
                           current_cat=cat_filter,
                           search_query=search_query,
                           page=page,
                           total_pages=total_pages,
                           total_items=total_items)  # Truyền thêm tổng số SP ra giao diện


@app.route('/product/<item_id>')
def product_detail(item_id):
    target_info = items_df.filter(pl.col("item_id") == item_id)
    if target_info.height == 0:
        return "Không tìm thấy sản phẩm", 404

    product = target_info.row(0, named=True)
    recs_co_occurrence = recommend_similar_items(
        item_id, transactions_df, items_df, top_n=6).to_dicts()
    recs_category = recommend_by_category_score(
        item_id, items_df, top_n=6).to_dicts()

    return render_template('detail.html', product=product, recs1=recs_co_occurrence, recs2=recs_category, categories=nav_categories)


@app.route('/cart')
def cart():
    return render_template('cart.html', categories=nav_categories)

# Đẩy hàm url_for ra ngoài giao diện để mã hóa link an toàn


@app.context_processor
def inject_url():
    return dict(url_for=url_for)


if __name__ == '__main__':
    app.run(debug=True, port=5000)
