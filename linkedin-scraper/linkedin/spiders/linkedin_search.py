import scrapy
import json

class LinkedSearchSpider(scrapy.Spider):
    name = "linkedin_search"
    api_url = f'https://www.linkedin.com/jobs/search?keywords=Sales%20Development%20Representative&location=United%20States&geoId=103644278&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum={0}' 
    company_links = []
    
    def start_requests(self):
        first_job_on_page = 0
        first_url = self.api_url.format(first_job_on_page)
        yield scrapy.Request(url=first_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})


    def parse_job(self, response):
        first_job_on_page = response.meta['first_job_on_page']

        job_item = {}
        jobs = response.css(".base-search-card__info")

        num_jobs_returned = len(jobs)
        print("******* Num Jobs Returned *******")
        print(num_jobs_returned)
        print('*****')
        
        for job in jobs:
            
            job_link = job.css("a::attr(href)").get(default='not-found').strip()
            job_title = job.css("h3::text").get(default='not-found').strip()
            self.company_links.append({'company_link': job_link, 'job_title': job_title})
            
        

        # if num_jobs_returned > 0 and first_job_on_page < 3:
        #     first_job_on_page = first_job_on_page + 1
        #     print(first_job_on_page)
        #     next_url = self.api_url.format(first_job_on_page)
        #     print('***************')
        #     print(next_url)
        #     yield scrapy.Request(url=next_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})
        
        
        print('***************')
        print('****** Scraping Complete')
        print('***************')
        with open('jobs.json', 'w') as outfile:
            json.dump(self.company_links, outfile, indent=4)
    

