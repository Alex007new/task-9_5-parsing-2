# Вариант-4

import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Set

from environs import Env
from itemadapter import ItemAdapter
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from intermark_scraper.models import Base, PropertiesRaw

logger = logging.getLogger(__name__)


def get_connection_string() -> str:
    env = Env()
    project_root = Path(__file__).resolve().parents[2]
    env.read_env(project_root / ".env")

    user = env.str("POSTGRES_USER")
    password = env.str("POSTGRES_PASSWORD")
    db = env.str("POSTGRES_DB")
    host = env.str("POSTGRES_HOST", "localhost")
    port = env.int("POSTGRES_PORT", 5432)

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def _parse_iso_dt(value: Optional[Any]) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    s = str(value).strip()
    return s == ""


def _merge_features(existing: Optional[Any], incoming: Optional[Any]) -> Optional[Any]:
    """
    features: JSONB
    - dict + dict: мерджим
      - images: объединяем уникально
      - params: мерджим ключи
      - params_list: объединяем уникально
      - остальные ключи: incoming перезапишет existing (если incoming не None)
    """
    if incoming is None:
        return existing
    if existing is None:
        return incoming

    if not isinstance(existing, dict) or not isinstance(incoming, dict):
        return incoming or existing

    out = dict(existing)

    # images
    ex_imgs = out.get("images") if isinstance(out.get("images"), list) else []
    in_imgs = incoming.get("images") if isinstance(incoming.get("images"), list) else []
    if ex_imgs or in_imgs:
        seen = set()
        merged = []
        for x in ex_imgs + in_imgs:
            if not x:
                continue
            if x in seen:
                continue
            seen.add(x)
            merged.append(x)
        out["images"] = merged

    # params (dict)
    ex_params = out.get("params") if isinstance(out.get("params"), dict) else {}
    in_params = incoming.get("params") if isinstance(incoming.get("params"), dict) else {}
    if ex_params or in_params:
        merged_params = dict(ex_params)
        for k, v in in_params.items():
            if v is None:
                continue
            # detail-стадия имеет приоритет: если ключ уже есть, можно перезаписать
            merged_params[k] = v
        out["params"] = merged_params

    # params_list (list)
    ex_pl = out.get("params_list") if isinstance(out.get("params_list"), list) else []
    in_pl = incoming.get("params_list") if isinstance(incoming.get("params_list"), list) else []
    if ex_pl or in_pl:
        seen = set()
        merged = []
        for x in ex_pl + in_pl:
            if not x:
                continue
            if x in seen:
                continue
            seen.add(x)
            merged.append(x)
        out["params_list"] = merged

    # остальные ключи
    for k, v in incoming.items():
        if k in {"images", "params", "params_list"}:
            continue
        if v is not None:
            out[k] = v

    return out


class DatabasePipeline:
    def __init__(self):
        self.engine = create_engine(
            get_connection_string(),
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        Base.metadata.create_all(self.engine)
        self.session_factory = scoped_session(
            sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        )

    @contextmanager
    def session_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.exception("DB operation failed: %s", e)
            raise
        finally:
            session.close()

    def open_spider(self, spider):
        """
        Готовим подсказки для Spider:
        - spider.db_urls: все url
        - spider.db_need_detail_urls: url, где надо дозаполнить detail (нет description)
        """
        db_urls: Set[str] = set()
        need_detail: Set[str] = set()

        with self.session_scope() as session:
            rows = session.query(PropertiesRaw.url, PropertiesRaw.description).all()
            for url, desc in rows:
                if not url:
                    continue
                db_urls.add(url)
                if _is_blank(desc):
                    need_detail.add(url)

        spider.db_urls = db_urls
        spider.db_need_detail_urls = need_detail

        logger.info("Loaded %s urls from DB into spider.db_urls", len(db_urls))
        logger.info("Need detail (missing description): %s", len(need_detail))

    def process_item(self, item, spider):
        a = ItemAdapter(item)

        url = a.get("url") or a.get("link")
        if not url:
            logger.warning("Skip item: url is empty. Item=%s", dict(a))
            return item

        scraped_at = _parse_iso_dt(a.get("scraped_at") or a.get("parsed_at"))

        incoming: Dict[str, Any] = {
            "url": url,
            "scraped_at": scraped_at,
            "source_page": a.get("source_page"),
            "title": a.get("title"),
            "location": a.get("location"),
            "price_raw": a.get("price_raw"),
            "area_raw": a.get("area_raw"),
            "object_id": a.get("object_id"),
            "description": a.get("description"),
            "features": a.get("features"),
        }

        new_desc = incoming.get("description")
        new_features = incoming.get("features") if isinstance(incoming.get("features"), dict) else None
        stage = None
        if new_features:
            stage = new_features.get("from")

        logger.info(
            "[pipeline] got item url=%s stage=%s desc_len=%s area_raw=%r",
            url,
            stage,
            0 if not new_desc else len(str(new_desc)),
            incoming.get("area_raw"),
        )

        with self.session_scope() as session:
            existing = session.query(PropertiesRaw).filter(PropertiesRaw.url == url).one_or_none()

            if not existing:
                # INSERT (первая стадия или новый объект)
                row = PropertiesRaw(
                    url=url,
                    source_page=incoming.get("source_page"),
                    title=incoming.get("title"),
                    location=incoming.get("location"),
                    price_raw=incoming.get("price_raw"),
                    area_raw=incoming.get("area_raw"),
                    object_id=incoming.get("object_id"),
                    description=incoming.get("description"),
                    features=incoming.get("features"),
                )
                if scraped_at is not None:
                    row.scraped_at = scraped_at
                session.add(row)
                session.flush()

                spider.db_urls.add(url)
                # если вставили без описания — попросим деталку
                if _is_blank(incoming.get("description")):
                    spider.db_need_detail_urls.add(url)

                logger.info("[pipeline] inserted url=%s", url)
                return item

            # UPDATE (важно: работаем даже если нет UNIQUE на url)
            changed = False

            # source_page/title/location/price/object_id/area_raw — добиваем если пусто
            for field in ["source_page", "title", "location", "price_raw", "object_id", "area_raw"]:
                new_val = incoming.get(field)
                old_val = getattr(existing, field)
                if _is_blank(old_val) and not _is_blank(new_val):
                    setattr(existing, field, new_val)
                    changed = True

            # description: детальная стадия имеет приоритет — обновляем, если incoming не пустой
            if not _is_blank(new_desc):
                if _is_blank(existing.description) or len(str(new_desc)) > len(str(existing.description or "")):
                    existing.description = new_desc
                    changed = True

            # features: мерджим (и это должно поднять from -> detail, если пришла деталка)
            merged = _merge_features(existing.features, incoming.get("features"))
            if merged != existing.features:
                existing.features = merged
                changed = True

            # scraped_at: можно хранить “последний парсинг”
            if scraped_at is not None:
                existing.scraped_at = scraped_at
                changed = True

            if changed:
                session.add(existing)
                session.flush()
                logger.info("[pipeline] updated url=%s (fields merged/filled)", url)
            else:
                logger.info("[pipeline] no changes url=%s", url)

            # обновим подсказки спайдеру на текущий ран
            spider.db_urls.add(url)
            if _is_blank(existing.description):
                spider.db_need_detail_urls.add(url)
            else:
                if url in spider.db_need_detail_urls:
                    spider.db_need_detail_urls.remove(url)

        return item

    def close_spider(self, spider):
        self.session_factory.remove()
        self.engine.dispose()
        logger.info("Database connection closed")
