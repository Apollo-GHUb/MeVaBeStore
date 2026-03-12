import polars as pl

# Giải pháp 1: Đề xuất dựa trên hành vi mua cùng


def recommend_similar_items(target_item_id, transactions, items, top_n=5):

    customers_who_bought = (
        transactions.filter(pl.col("item_id") == target_item_id)
        .select("customer_id").unique()
    )
    if customers_who_bought.height == 0:
        return pl.DataFrame()

    co_purchases = (
        transactions.join(customers_who_bought, on="customer_id", how="inner")
        .filter(pl.col("item_id") != target_item_id)
    )

    top_recs = (
        co_purchases.group_by("item_id")
        .agg(pl.len().alias("so_lan_mua_cung"))
        .sort("so_lan_mua_cung", descending=True)
        .head(top_n)
    )
    return top_recs.join(items, on="item_id", how="left")

# Giải pháp 2: Đề xuất dựa trên tính điểm danh mục


def recommend_by_category_score(target_item_id, items, top_n=5):

    target_item = items.filter(pl.col("item_id") == target_item_id)
    if target_item.height == 0:
        return pl.DataFrame()

    target_row = target_item.row(0, named=True)

    t_cat_l3 = target_row.get("category_l3")       # Cấp 3: Nhóm SP
    t_cat_l2 = target_row.get("category_l2")       # Cấp 2: Ngành hàng phụ
    t_cat_l1 = target_row.get("category_l1")       # Cấp 1: Ngành hàng chính

    other_items = items.filter(pl.col("item_id") != target_item_id)
    scored_items = other_items.with_columns(pl.lit(0).alias("category_score"))

    # Tiêu chí 1: Khớp Category_L3 +10 điểm
    if t_cat_l3:
        scored_items = scored_items.with_columns(
            pl.when(pl.col("category_l3") == t_cat_l3).then(pl.col(
                "category_score") + 10).otherwise(pl.col("category_score")).alias("category_score")
        )

    # Tiêu chí 2: Khớp Category_L2 -> +5 điểm
    if t_cat_l2:
        scored_items = scored_items.with_columns(
            pl.when(pl.col("category_l2") == t_cat_l2).then(pl.col(
                "category_score") + 5).otherwise(pl.col("category_score")).alias("category_score")
        )

    # Tiêu chí 3: Khớp Category_L1 -> +2 điểm
    if t_cat_l1:
        scored_items = scored_items.with_columns(
            pl.when(pl.col("category_l1") == t_cat_l1).then(pl.col(
                "category_score") + 2).otherwise(pl.col("category_score")).alias("category_score")
        )

    # Lọc điểm > 0 và lấy Top N
    recommendations = (
        scored_items.filter(pl.col("category_score") > 0)
        .sort("category_score", descending=True)
        .head(top_n)
    )
    return recommendations
