import scrapy
import json

class LinkedCompanyJobsSpider(scrapy.Spider):
    name = "linkedin_company_jobs"
    api_url = 'https://www.linkedin.com/jobs/sunstates-security-jobs-worldwide?f_C=349053&trk=top-card_top-card-primary-button-top-card-primary-cta'
    job_links = []
    
    def start_requests(self):
        first_job_on_page = 0
        first_url = self.api_url.format(first_job_on_page)
        yield scrapy.Request(url=first_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})


    def parse_job(self, response):
        first_job_on_page = response.meta['first_job_on_page']

        jobs = response.css(".base-card__full-link")

        num_jobs_returned = len(jobs)
        print("******* Num Jobs Returned *******")
        print(num_jobs_returned)
        print('*****')
        
        for job in jobs:
            
            job_link = job.css("a::attr(href)").get(default='not-found').strip()
            self.job_links.append(job_link)
            
        

        # if num_jobs_returned > 0 and first_job_on_page < 3:
        #     first_job_on_page = first_job_on_page + 1
        #     print(first_job_on_page)
        #     next_url = self.api_url.format(first_job_on_page)
        #     print('***************')
        #     print(next_url)
        #     yield scrapy.Request(url=next_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})
        with open('jobs.json', 'w') as outfile:
            json.dump(self.job_links, outfile, indent=4)
    

