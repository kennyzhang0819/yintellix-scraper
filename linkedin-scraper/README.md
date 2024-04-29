
```python

virtual-env\Scripts\activate

```

To use the ScrapeOps Proxy you need to first install the proxy middleware:

```python

pip install scrapeops-scrapy-proxy-sdk

```

Then activate the ScrapeOps Proxy by adding your API key to the `SCRAPEOPS_API_KEY` in the ``settings.py`` file.

```python

SCRAPEOPS_API_KEY = 'YOUR_API_KEY'

SCRAPEOPS_PROXY_ENABLED = True

DOWNLOADER_MIDDLEWARES = {
    'scrapeops_scrapy_proxy_sdk.scrapeops_scrapy_proxy_sdk.ScrapeOpsScrapyProxySdk': 725,
}

```


## ScrapeOps Monitoring
To monitor our scraper, this spider uses the [ScrapeOps Monitor](https://scrapeops.io/monitoring-scheduling/), a free monitoring tool specifically designed for web scraping. 

**Live demo here:** [ScrapeOps Demo](https://scrapeops.io/app/login/demo) 

![ScrapeOps Dashboard](https://scrapeops.io/assets/images/scrapeops-promo-286a59166d9f41db1c195f619aa36a06.png)

To use the ScrapeOps Proxy you need to first install the monitoring SDK:

```

pip install scrapeops-scrapy

```


Then activate the ScrapeOps Proxy by adding your API key to the `SCRAPEOPS_API_KEY` in the ``settings.py`` file.

```python

SCRAPEOPS_API_KEY = 'YOUR_API_KEY'

# Add In The ScrapeOps Monitoring Extension
EXTENSIONS = {
'scrapeops_scrapy.extension.ScrapeOpsMonitor': 500, 
}


DOWNLOADER_MIDDLEWARES = {

    ## ScrapeOps Monitor
    'scrapeops_scrapy.middleware.retry.RetryMiddleware': 550,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    
    ## Proxy Middleware
    'scrapeops_scrapy_proxy_sdk.scrapeops_scrapy_proxy_sdk.ScrapeOpsScrapyProxySdk': 725,
}

```