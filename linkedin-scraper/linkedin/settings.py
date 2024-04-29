# Scrapy settings for linkedin project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'linkedin'

SPIDER_MODULES = ['linkedin.spiders']
NEWSPIDER_MODULE = 'linkedin.spiders'

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_MAX_DELAY = 2
DOWNLOAD_DELAY = 0

USER_AGENT = 'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148'

# HTTPCACHE_ENABLED = True

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

SCRAPEOPS_API_KEY = '4009e3f5-6658-4b58-86a6-0e86d02ce488'

SCRAPEOPS_PROXY_ENABLED = False

RETRY_HTTP_CODES = [429]


DOWNLOADER_MIDDLEWARES = {
    # "linkedin.middlewares.TooManyRequestsRetryMiddleware": 543,
    # "scrapy.spidermiddlewares.offsite.OffsiteMiddleware": None,

#     ## ScrapeOps Monitor
#     'scrapeops_scrapy.middleware.retry.RetryMiddleware': 550,
#     'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    
#     ## Proxy Middleware
    # 'scrapeops_scrapy_proxy_sdk.scrapeops_scrapy_proxy_sdk.ScrapeOpsScrapyProxySdk': 725,
}

CONCURRENT_REQUESTS = 1000
LOG_LEVEL = "DEBUG"