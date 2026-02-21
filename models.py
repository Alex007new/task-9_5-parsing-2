from sqlalchemy import (
    Column,
    Integer,
    Text,
    DateTime,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class PropertiesRaw(Base):
    """
    Сырой слой по объявлениям Intermark.

    Храним:
    - обязательные поля: url, scraped_at
    - текстовый контент: title, location, description
    - цены/площади в raw-виде (чтобы не потерять формат)
    - вложенные структуры (характеристики/фичи) в JSONB
    """
    __tablename__ = "properties_raw"
    __table_args__ = {"schema": "intermark"}

    id = Column(Integer, primary_key=True, autoincrement=True)

    # обязательные
    url = Column(Text, nullable=False, unique=True)
    scraped_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # минимум 5+ полей
    source_page = Column(Text, nullable=True)       # страница каталога, где нашли объект
    title = Column(Text, nullable=True)             # заголовок карточки/объекта
    location = Column(Text, nullable=True, index=True)          # город/регион/страна
    price_raw = Column(Text, nullable=True)         # "€ 896 000 – 1 682 000"
    area_raw = Column(Text, nullable=True)          # "2700 м²" (если есть)
    object_id = Column(Text, nullable=True, index=True)         # "ID 724599" (если есть)
    description = Column(Text, nullable=True)       # описание (если на странице есть)

    # вложенные структуры
    features = Column(JSONB, nullable=True)         # dict/list: характеристики, теги, параметры
