# task-9_5-parsing-2







##### 1. Scrapy
scrapy crawl intermark_spain

# 2. Spark ETL
spark-submit --packages org.postgresql:postgresql:42.7.3 spark/etl_properties.py

# 3. Проверка результата
SELECT COUNT(*) FROM intermark.properties_clean;
