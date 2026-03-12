import polars as pl


def recommend_similar_items(target_item_id, transactions, items, top_n=5):
    customers_who_bought = (
        transactions.filter(pl.col("item_id") == target_item_id)
        .select("customer_id").unique()
    )

    top_recs = pl.DataFrame()

    # Nếu có người mua, tìm sản phẩm mua cùng
    if customers_who_bought.height > 0:
        co_purchases = (
            transactions.join(customers_who_bought,
                              on="customer_id", how="inner")
            .filter(pl.col("item_id") != target_item_id)
        )
        if co_purchases.height > 0:
            top_recs = (
                co_purchases.group_by("item_id")
                .agg(pl.len().alias("so_lan_mua_cung"))
                .sort("so_lan_mua_cung", descending=True)
                .head(top_n)
            )

    # CƠ CHẾ DỰ PHÒNG: Nếu dữ liệu quá ít không đủ gợi ý, lấy sản phẩm bán chạy nhất bù vào
    if top_recs.height < top_n:
        needed = top_n - top_recs.height
        popular = (
            transactions.filter(pl.col("item_id") != target_item_id)
            .group_by("item_id").agg(pl.len().alias("so_lan_mua_cung"))
            .sort("so_lan_mua_cung", descending=True)
        )
        # Bỏ qua những SP đã có trong top_recs
        if top_recs.height > 0:
            popular = popular.filter(~pl.col("item_id").is_in(
                top_recs.get_column("item_id")))

        fallback_recs = popular.head(needed)
        if fallback_recs.height > 0:
            if top_recs.height > 0:
                top_recs = pl.concat([top_recs, fallback_recs])
            else:
                top_recs = fallback_recs

    if top_recs.height == 0:
        return pl.DataFrame()

    return top_recs.join(items, on="item_id", how="left")


def recommend_by_category_score(target_item_id, items, top_n=5):
    target_item = items.filter(pl.col("item_id") == target_item_id)
    if target_item.height == 0:
        return pl.DataFrame()

    target_row = target_item.row(0, named=True)
    t_cat_l3 = target_row.get("category_l3")
    t_cat_l2 = target_row.get("category_l2")
    t_cat_l1 = target_row.get("category_l1")

    other_items = items.filter(pl.col("item_id") != target_item_id)
    scored_items = other_items.with_columns(pl.lit(0).alias("category_score"))

    if t_cat_l3:
        scored_items = scored_items.with_columns(
            pl.when(pl.col("category_l3") == t_cat_l3).then(pl.col(
                "category_score") + 10).otherwise(pl.col("category_score")).alias("category_score")
        )
    if t_cat_l2:
        scored_items = scored_items.with_columns(
            pl.when(pl.col("category_l2") == t_cat_l2).then(pl.col(
                "category_score") + 5).otherwise(pl.col("category_score")).alias("category_score")
        )
    if t_cat_l1:
        scored_items = scored_items.with_columns(
            pl.when(pl.col("category_l1") == t_cat_l1).then(pl.col(
                "category_score") + 2).otherwise(pl.col("category_score")).alias("category_score")
        )

    recommendations = (
        scored_items.filter(pl.col("category_score") > 0)
        .sort("category_score", descending=True)
        .head(top_n)
    )
    return recommendations
