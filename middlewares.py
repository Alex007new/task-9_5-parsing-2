# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class IntermarkScraperSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # matching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class IntermarkScraperDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


import random
from fake_useragent import UserAgent


# class RotateUserAgentMiddleware:
#     """
#     Меняет User-Agent на каждый запрос.
#     """
#
#     def __init__(self):
#         self.ua = UserAgent()
#
#     @classmethod
#     def from_crawler(cls, crawler):
#         return cls()
#
#     def process_request(self, request, spider):
#         request.headers["User-Agent"] = self.ua.random

import random


class RotateUserAgentMiddleware:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_request(self, request, spider):
        request.headers["User-Agent"] = random.choice(self.USER_AGENTS)


import time
import random
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message


class SmartRetryMiddleware(RetryMiddleware):
    """
    Ретрай для временных/антибот статусов с backoff-паузой.
    Работает вместе со стандартным RetryMiddleware (мы его отключаем в settings).
    """

    RETRY_HTTP_CODES = {403, 408, 429, 500, 502, 503, 504}

    def __init__(self, settings):
        super().__init__(settings)
        self.max_backoff = settings.getfloat("SMART_RETRY_MAX_BACKOFF", 15.0)
        self.base_backoff = settings.getfloat("SMART_RETRY_BASE_BACKOFF", 1.5)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_response(self, request, response, spider):
        if request.meta.get("dont_retry", False):
            return response

        if response.status in self.RETRY_HTTP_CODES:
            retries = request.meta.get("retry_times", 0) + 1
            if retries <= self.max_retry_times:
                # backoff: 1.5, 3, 6, 12... + jitter
                backoff = min(self.base_backoff * (2 ** (retries - 1)), self.max_backoff)
                jitter = random.uniform(0, 0.5)
                sleep_s = backoff + jitter

                spider.logger.info(
                    "[smart-retry] %s status=%s retry=%s/%s sleep=%.2fs",
                    request.url,
                    response.status,
                    retries,
                    self.max_retry_times,
                    sleep_s,
                )
                time.sleep(sleep_s)

                reason = response_status_message(response.status)
                return self._retry(request, reason, spider) or response

        return response

