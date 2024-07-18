from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, lit

def get_product_categories(spark, products_df, categories_df, product_categories_df):
    """
    Возвращает датафрейм с парами "Имя продукта - Имя категории" и именами продуктов без категорий.

    Args:
        spark: SparkSession.
        products_df: DataFrame с продуктами.
        categories_df: DataFrame с категориями.
        product_categories_df: DataFrame с связями продуктов и категорий.

    Returns:
        DataFrame с парами "Имя продукта - Имя категории" и именами продуктов без категорий.
    """

    # Соединяем таблицу продуктов и таблицу связей
    product_categories_joined = products_df.join(product_categories_df, on='product_id', how='left')

    # Соединяем таблицу категорий и таблицу связей
    categories_joined = categories_df.join(product_categories_df, on='category_id', how='left')

    # Агрегируем категории для каждого продукта
    product_categories_grouped = product_categories_joined.groupBy('product_id').agg(
        collect_set('category_id').alias('category_ids')
    )

    # Агрегируем продукты для каждой категории
    category_products_grouped = categories_joined.groupBy('category_id').agg(
        collect_set('product_id').alias('product_ids')
    )

    # Соединяем таблицу с продуктами и таблицу с категориями
    products_categories_merged = product_categories_grouped.join(
        product_categories_df, on='product_id', how='left'
    ).select(
        'product_id', 'category_ids', 'category_id'
    )

    # Соединяем таблицу с продуктами и категориями и таблицу категорий
    products_categories_final = products_categories_merged.join(
        categories_df, on='category_id', how='left'
    ).select(
        'product_id', 'category_ids', 'category_name'
    )

    # Фильтруем продукты без категорий
    products_without_categories = products_df.filter(
        col('product_id').isin(product_categories_grouped.filter(col('category_ids').isNull()).select('product_id').collect())
    ).select(
        'product_name'
    )

    # Объединяем результаты
    final_df = products_categories_final.union(products_without_categories.withColumn('category_name', lit(None)))

    return final_df

# Пример использования
spark = SparkSession.builder.appName("ProductCategories").getOrCreate()

products_data = [
    (1, "Продукт A"),
    (2, "Продукт B"),
    (3, "Продукт C")
]
products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])

categories_data = [
    (1, "Категория 1"),
    (2, "Категория 2"),
    (3, "Категория 3")
]
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])

product_categories_data = [
    (1, 1),
    (1, 2),
    (2, 1),
    (3, 3)
]
product_categories_df = spark.createDataFrame(product_categories_data, ["product_id", "category_id"])

result_df = get_product_categories(spark, products_df, categories_df, product_categories_df)

result_df.show()