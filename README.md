# task-9_5-parsing-2

# ETL-пайплайн для парсинга и обработки сайта по продаже недвижимости  

## 🎯 Цель проекта  

Разработать масштабируемый ETL-пайплайн для:  
- Парсинга динамического сайта (https://intermark.ru/nedvizhimost-za-rubezhom/investicii-spain) с использованием Scrapy
- Сохранения сырых данных в PostgreSQL (через SQLAlchemy)
- Последующей очистки и трансформации данных в PySpark
- Загрузки очищенных данных обратно в PostgreSQL в нормализованную структуру

## ⚙️ Используемые технологии  

- Python 3.x
- Scrapy
- Selenium (для динамического контента)
- PostgreSQL
- SQLAlchemy
- Apache Spark (PySpark 4.x)
- Docker (контейнеры: Postgres + Spark Notebook)
- WSL2

## 🏗 Архитектура решения  

Scrapy Spider  
     ↓  
PostgreSQL (intermark.properties_raw)  
     ↓  
PySpark ETL (очистка, дедупликация, нормализация)  
     ↓  
PostgreSQL (intermark.properties_clean)    

## 📁 Структура проекта  

<img width="290" height="489" alt="image" src="https://github.com/user-attachments/assets/363e28c3-f578-4909-8f96-cc064c8ca1f9" />

    
  

  




## 🎯 Минимальный набор команд  

  
#### 1. Scrapy
cd scrapy_project  
scrapy crawl intermark_spain -s LOG_FILE=logs/scrapy_run.log  

#### 2. Spark ETL
spark-submit --packages org.postgresql:postgresql:42.7.3 spark/etl_properties.py

#### 3. Проверка результата
SELECT COUNT(*) FROM intermark.properties_clean;  
SELECT * FROM intermark.properties_clean;  

  
