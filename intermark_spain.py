# Вариант 7 (fix pagination)

import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import scrapy
from scrapy.http import HtmlResponse

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def _unique_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _set_query_param(url: str, key: str, value: str) -> str:
    """
    Надёжно добавляет/заменяет query-параметр в URL.
    Пример: .../investicii-spain?page=2
    """
    u = urlparse(url)
    q = parse_qs(u.query)
    q[key] = [value]
    new_query = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


class IntermarkSpainSpider(scrapy.Spider):
    name = "intermark_spain"
    allowed_domains = ["intermark.ru"]
    start_urls = ["https://intermark.ru/nedvizhimost-za-rubezhom/investicii-spain"]

    custom_settings = {
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 1,
        "RETRY_TIMES": 5,
        "DOWNLOAD_TIMEOUT": 30,

        # ВАЖНО: иначе detail (/objects/...) может НЕ скачиваться из-за robots.txt
        "ROBOTSTXT_OBEY": False,

        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Эти поля заполняет pipeline в open_spider()
        self.db_urls: Set[str] = set()  # все url из БД
        self.db_need_detail_urls: Set[str] = set()  # url, где нужно дозаполнить detail (нет description/area_raw)

        # внутреннее
        self._driver: Optional[webdriver.Chrome] = None
        self._listing_visited: Set[str] = set()

    # -------------------------
    # Selenium lifecycle
    # -------------------------
    def open_spider(self, spider):
        """
        Не обязателен (Scrapy не всегда зовёт open_spider у Spider),
        но пусть будет: если вызовется — мы точно поднимем драйвер заранее.
        """
        self._init_driver()

    def closed(self, reason):
        """
        Правильный хук Scrapy для завершения паука.
        Не переопределяем close(), чтобы не ловить TypeError из signal handler.
        """
        self._quit_driver()

    def _init_driver(self) -> None:
        if self._driver is not None:
            return

        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1600,900")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--lang=ru-RU")

        service = Service(ChromeDriverManager().install())
        self._driver = webdriver.Chrome(service=service, options=options)
        self.logger.info("[selenium] driver initialized")

    def _quit_driver(self) -> None:
        if self._driver is None:
            return
        try:
            self._driver.quit()
            self.logger.info("[selenium] driver quit ok")
        except Exception as e:
            self.logger.warning("[selenium] driver quit error: %s", e)
        finally:
            self._driver = None

    def _get_selenium_listing_response(self, url: str, max_scrolls: int = 4) -> HtmlResponse:
        """
        Открываем listing через Selenium, ждём контент/карточки, скроллим, чтобы прогрузился AJAX,
        возвращаем HtmlResponse (как будто это ответ Scrapy).

        Важно: НЕ ПАДАЕМ на TimeoutException — возвращаем страницу как есть (cards может быть 0).
        Это нужно, чтобы корректно остановить пагинацию (page=3 может быть пустой/другой шаблон).
        """
        self._init_driver()
        assert self._driver is not None

        self.logger.info("[selenium] GET %s", url)

        try:
            self._driver.get(url)
        except WebDriverException as e:
            self.logger.error("[selenium] get() failed url=%s err=%s", url, e)
            # Возвращаем пустой ответ, чтобы паук не падал
            return HtmlResponse(url=url, body=b"", encoding="utf-8")

        wait = WebDriverWait(self._driver, 15)

        # Ждём либо карточки, либо хотя бы body (страница отрисовалась).
        # Если карточек нет — это может быть "последняя страница" или "пустая выдача".
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        except TimeoutException:
            self.logger.warning("[selenium] body timeout on %s", url)

        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.object-card")))
        except TimeoutException:
            # Не падаем — просто логируем. Ниже мы всё равно снимем page_source и вернём HtmlResponse.
            self.logger.info("[selenium] no object-card found (timeout) on %s", url)

        prev_cnt = -1
        stable_hits = 0

        for i in range(1, max_scrolls + 1):
            try:
                self._driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except WebDriverException as e:
                self.logger.warning("[selenium] scroll failed url=%s err=%s", url, e)
                break

            time.sleep(0.7)

            try:
                cards_cnt = len(self._driver.find_elements(By.CSS_SELECTOR, "div.object-card"))
            except WebDriverException:
                cards_cnt = 0

            self.logger.info("[selenium] scroll=%s cards=%s", i, cards_cnt)

            if cards_cnt == prev_cnt:
                stable_hits += 1
            else:
                stable_hits = 0

            prev_cnt = cards_cnt
            if stable_hits >= 1:
                break

        html = self._driver.page_source or ""
        return HtmlResponse(url=url, body=html.encode("utf-8"), encoding="utf-8")

    # -------------------------
    # 2-stage crawling
    # -------------------------
    def start_requests(self):
        # Listing всегда берём Selenium-ом (динамика)
        for url in self.start_urls:
            if url in self._listing_visited:
                continue
            self._listing_visited.add(url)
            response = self._get_selenium_listing_response(url)
            yield from self.parse_listing(response)

    def parse_listing(self, response: HtmlResponse):
        """
        Stage 1: listing
        - собираем базовые поля
        - сохраняем listing-features (images + params)
        - решаем, идти ли на detail
        """
        if response.url == self.start_urls[0]:
            # для дебага
            with open("intermark_page.html", "wb") as f:
                f.write(response.body)
            self.logger.info("[listing] Saved HTML to intermark_page.html")

        self.logger.info("[listing] url=%s len(html)=%s", response.url, len(response.text))

        cards = response.css("div.object-card")
        self.logger.info("[listing] Found %s cards", len(cards))

        scraped_at = _now_iso()

        for card in cards:
            link = card.css("a.object-card-main-info__link::attr(href)").get()
            if not link:
                link = card.css('a[href*="/objects/"]::attr(href)').get()

            url = response.urljoin(link) if link else None
            if not url:
                continue

            id_text = _clean_text(card.css("div.object-card-main-info__id::text").get())
            object_id = None
            if id_text:
                m = re.search(r"\d+", id_text)
                object_id = m.group(0) if m else None

            title = (
                _clean_text(card.css("div.object-card-main-info__name-title div.name::text").get())
                or _clean_text(card.css("div.name::text").get())
            )

            location = (
                _clean_text(card.css("div.object-card-main-info__name-title div.address::text").get())
                or _clean_text(card.css("div.address::text").get())
            )

            price_raw = (
                _clean_text(card.css("div.object-card-main-info__price::text").get())
                or _clean_text(card.css('[class*="price"]::text').get())
            )

            # listing area_raw (иногда встречается в параметрах карточки)
            # params_text = " ".join(_clean_text(x) or "" for x in card.css("ul.object-card-param-list ::text").getall())
            # m_area = re.search(r"(\d[\d\s]{0,10})\s*(?:м²|m²)", params_text, flags=re.IGNORECASE)
            # area_raw = None
            # if m_area:
            #     area_raw = _clean_text(m_area.group(0))
            # Площадь: ищем по ТЕКСТУ всей карточки (надежнее, чем только ul.object-card-param-list)
            card_text = " ".join(card.css("::text").getall())
            card_text = re.sub(r"\s+", " ", card_text)

            m_area = re.search(r"(\d[\d\s]{0,10})\s*(?:м²|m²)", card_text, flags=re.IGNORECASE)
            area_raw = _clean_text(m_area.group(0)) if m_area else None

            # images (listing)
            imgs = card.css("picture img::attr(src), picture img::attr(data-lazy)").getall()
            imgs = _unique_keep_order([response.urljoin(x) for x in imgs if x])

            # params (listing) - как текстовые строки
            params_list = [
                _clean_text(" ".join(li.css("::text").getall()))
                for li in card.css("ul.object-card-param-list li")
            ]
            params_list = [x for x in params_list if x]

            listing_item: Dict[str, Any] = {
                "url": url,
                "source_page": response.url,
                "scraped_at": scraped_at,  # обязательное поле по ТЗ
                "object_id": object_id,
                "title": title,
                "location": location,
                "price_raw": price_raw,
                "area_raw": area_raw,
                "description": None,  # на listing может отсутствовать; деталь дозаполнит
                "features": {
                    "from": "listing",
                    "images": imgs,
                    "params_list": params_list,
                },
            }

            # 1) Всегда отдаём listing-item: pipeline сам решит insert/update и смержит features.
            yield listing_item

            # 2) Решаем, идти ли на detail:
            #    - если в БД нет строки
            #    - или pipeline сказал "нужно дозаполнить detail" (нет description/area_raw)
            need_detail = (url not in self.db_urls) or (url in self.db_need_detail_urls)

            if need_detail:
                yield response.follow(
                    url,
                    callback=self.parse_detail,
                    meta={"listing_item": listing_item},
                    dont_filter=True,
                )

        # -------------------------
        # ПАГИНАЦИЯ: надёжно через ?page=N
        # -------------------------
        # Если карточек на текущей странице 0 — это уже сигнал остановки (например, page=3 пустая).
        if len(cards) == 0:
            self.logger.info("[pagination] stop: 0 cards on current page %s", response.url)
            return

        u = urlparse(response.url)
        q = parse_qs(u.query)
        cur_page = 1
        if "page" in q and q["page"]:
            try:
                cur_page = int(q["page"][0])
            except Exception:
                cur_page = 1

        next_url = _set_query_param(response.url, "page", str(cur_page + 1))

        if next_url in self._listing_visited:
            self.logger.info("[pagination] already visited: %s", next_url)
            return

        next_resp = self._get_selenium_listing_response(next_url)
        next_cards = next_resp.css("div.object-card")
        self.logger.info("[pagination] try next_url=%s cards=%s", next_url, len(next_cards))

        if len(next_cards) > 0:
            self._listing_visited.add(next_url)
            yield from self.parse_listing(next_resp)
        else:
            self.logger.info("[pagination] stop: no cards on %s", next_url)

    def parse_detail(self, response: HtmlResponse):
        """
        Stage 2: detail
        - дозаполняем description, area_raw и доп. features
        - если description не найден в Scrapy-ответе, делаем Selenium fallback
        """
        listing_item: Dict[str, Any] = response.meta.get("listing_item") or {}
        scraped_at = _now_iso()

        def extract_description(resp: HtmlResponse) -> Optional[str]:
            # 1) meta description (часто есть, но иногда пусто/не то)
            desc = resp.xpath('//meta[@name="description"]/@content').get()
            desc = _clean_text(desc)
            if desc:
                return desc

            # 2) пробуем “видимые” блоки описания (селекторы могут отличаться — пробуем набором)
            candidates = []

            # частые варианты: description/desc/text/content
            candidates += resp.css('[class*="description"] ::text').getall()
            candidates += resp.css('[class*="desc"] ::text').getall()
            candidates += resp.css('[class*="text"] ::text').getall()
            candidates += resp.css('article ::text').getall()
            candidates += resp.css('main ::text').getall()

            # склеиваем и чистим
            txt = _clean_text(" ".join(candidates))
            return txt

        description = extract_description(response)

        # --- Selenium fallback, если Scrapy не увидел описание (AJAX/динамика) ---
        if not description:
            try:
                self._init_driver()
                assert self._driver is not None
                self.logger.info("[detail][selenium-fallback] GET %s", response.url)
                self._driver.get(response.url)

                wait = WebDriverWait(self._driver, 15)
                # ждём что-нибудь “контентное”: заголовок/описание/любой крупный блок
                wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "body")
                    )
                )
                time.sleep(1.2)  # небольшая пауза на догрузку текста

                html = self._driver.page_source
                resp2 = HtmlResponse(url=response.url, body=html.encode("utf-8"), encoding="utf-8")
                description = extract_description(resp2)

                self.logger.info(
                    "[detail][selenium-fallback] description_len=%s",
                    0 if not description else len(description)
                )
            except Exception as e:
                self.logger.warning("[detail][selenium-fallback] failed: %s", e)

        # area_raw: пытаемся найти в тексте страницы (Scrapy)
        area_raw = None
        page_text = " ".join(response.css("body ::text").getall())
        page_text = re.sub(r"\s+", " ", page_text)
        m_area = re.search(r"(\d[\d\s]{0,10})\s*(?:м²|m²)", page_text, flags=re.IGNORECASE)
        if m_area:
            area_raw = _clean_text(m_area.group(0))

        # images: detail
        imgs = response.css("picture img::attr(src), picture img::attr(data-lazy)").getall()
        imgs = _unique_keep_order([response.urljoin(x) for x in imgs if x])

        # params: detail (пары ключ-значение пытаемся вытащить из li)
        params: Dict[str, str] = {}
        for li in response.css("ul li"):
            txt = _clean_text(" ".join(li.css("::text").getall()))
            if not txt or ":" not in txt:
                continue
            k, v = txt.split(":", 1)
            k = _clean_text(k)
            v = _clean_text(v)
            if k and v and k not in params:
                params[k] = v

        detail_item: Dict[str, Any] = {
            "url": listing_item.get("url") or response.url,
            "source_page": listing_item.get("source_page"),
            "scraped_at": scraped_at,
            "object_id": listing_item.get("object_id"),
            "title": listing_item.get("title"),
            "location": listing_item.get("location"),
            "price_raw": listing_item.get("price_raw"),
            "area_raw": area_raw or listing_item.get("area_raw"),
            "description": description,  # <-- теперь реально пытаемся добыть
            "features": {
                "from": "detail",
                "images": imgs,
                "params": params,
            },
        }

        self.logger.info(
            "[detail] url=%s description_len=%s",
            detail_item["url"],
            0 if not description else len(description)
        )

        yield detail_item
