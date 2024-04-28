import scrapy
import json
import pandas as pd

class LinkedCompanyJobsSpider(scrapy.Spider):
    name = "linkedin_company_jobs"
    
    company_job_links = []
    
    def start_requests(self):
        df = pd.read_csv('company_profiles.csv')
        df = df[["name", "job_link"]]
        for link in self.df['job_link']:
            yield scrapy.Request(url=link, callback=self.parse_job)
        df = pd.DataFrame(self.company_job_links)
        df.to_csv('company_jobs_relation.csv', encoding='utf-8', index=False)
        
    def parse_job(self, response):

        jobs = response.css(".base-search-card__info")

        num_jobs_returned = len(jobs)
        print("******* Num Jobs Returned *******")
        print(num_jobs_returned)
        print('*****')
        
        for job in jobs:
            company_name = job.css(".base-search-card__subtitle a::text").get(default='not-found').strip()
            job_link = job.css("a::attr(href)").get(default='not-found').strip()
            job_title = job.css("h3::text").get(default='not-found').strip()
            self.company_job_links.append({'company_name': company_name, 'job_link': job_link, 'job_title': job_title})

        
        
    


    

