
## scrapeops
from scrapeops_python_logger.stats.model import OverallStatsModel, PeriodicStatsModel 
from scrapeops_python_logger.utils import utils
from scrapeops_python_logger.normalizer.exceptions import ExceptionNormalizer  
from scrapeops_python_logger.normalizer.request_response import SOPSResponse

import copy


class StatsLogger(OverallStatsModel, PeriodicStatsModel):


    def display_stats(self):
        self.display_periodic_stats()
        self.display_overall_stats()


    def check_periodic_stats(self):
        if self._periodic_stats == {}:
            self.set_value(PeriodicStatsModel._periodic_stats, 'job_id', self.job_id)


    def spider_open_stats(self):
        self.set_value(OverallStatsModel._overall_stats, 'job_id', self.job_id)
        self.set_value(OverallStatsModel._overall_stats, 'job_name', self.job_group_name)
        self.set_value(OverallStatsModel._overall_stats, 'job_start_time', self.start_time)
        self.set_value(OverallStatsModel._overall_stats, 'job_finish_time', 0)
        self.set_value(OverallStatsModel._overall_stats, 'job_run_time', 0)
        self.set_value(OverallStatsModel._overall_stats, 'status', 'Live')
        self.set_value(OverallStatsModel._overall_stats, 'middleware_enabled', False)
        


    def spider_close_stats( self, reason=None):
        finish_time = utils.current_time()
        self.aggregate_stats() 
        self.set_value(OverallStatsModel._overall_stats, 'job_finish_time', finish_time)
        self.set_value(OverallStatsModel._overall_stats, 'job_run_time', finish_time - self.start_time)
        self.set_value(OverallStatsModel._overall_stats, 'status', 'Finished')
        self.set_value(OverallStatsModel._overall_stats, 'reason', reason)
        self.set_value(OverallStatsModel._overall_stats, 'period_frequency', self._period_frequency)



    def generate_request_stats(self, request_response_object):
        proxy_name = request_response_object.get_proxy_name()
        proxy_setup = request_response_object.get_proxy_setup()
        domain_name = request_response_object.get_domain()
        page_type = request_response_object.get_page_type()
        custom_tag = request_response_object.get_custom_tag()
        method = request_response_object.get_request_method()
        
        ## periodic stats
        self.check_periodic_stats()
        self.inc_value(self._periodic_stats, f'requests|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{custom_tag}|count')

        ## overall stats
        self.inc_value(OverallStatsModel._overall_stats, f'requests|{method}|count')
        self.inc_value(OverallStatsModel._overall_stats, f'requests|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{custom_tag}|count')


    def generate_response_stats(self, request_response_object):
        proxy_name = request_response_object.get_proxy_name()
        proxy_setup = request_response_object.get_proxy_setup()
        domain_name = request_response_object.get_domain()
        page_type = request_response_object.get_page_type()
        validation = request_response_object.get_validation_test()
        geo = request_response_object.get_geo()
        custom_tag = request_response_object.get_custom_tag()
        custom_signal = 'none'
        bytes=request_response_object.get_bytes()
        total_latency = request_response_object.get_latency()
        method = request_response_object.get_request_method()
        status_code = request_response_object.get_status_code()

        #if the request is not of type 2xx then log it in the failed urls array
        if(str(request_response_object.get_status_code())[:1] != '2'):
            self.failed_urls.append(request_response_object.get_raw_url())
        else:
            if request_response_object.get_raw_url() in self.failed_urls:
                self.failed_urls.remove(request_response_object.get_raw_url())

        ## periodic stats
        self.check_periodic_stats()
        self.inc_value(PeriodicStatsModel._periodic_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|count')
        self.inc_value(PeriodicStatsModel._periodic_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|bytes', count=bytes)
        self.inc_value(PeriodicStatsModel._periodic_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|total_latency', count=total_latency)
        self.min_value(PeriodicStatsModel._periodic_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|min_latency', total_latency)
        self.max_value(PeriodicStatsModel._periodic_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|max_latency', total_latency)

        ## overall stats
        self.inc_value(OverallStatsModel._overall_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|count')
        self.inc_value(OverallStatsModel._overall_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|bytes', count=bytes)
        self.inc_value(OverallStatsModel._overall_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|total_latency', count=total_latency)
        self.min_value(OverallStatsModel._overall_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|min_latency', total_latency)
        self.max_value(OverallStatsModel._overall_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|max_latency', total_latency)


    def generate_item_stats(self, request_response_object, signal=None):
        if isinstance(request_response_object, SOPSResponse):
            method = request_response_object.get_request_method()
            status_code = request_response_object.get_status_code()
            proxy_name = request_response_object.get_proxy_name()
            proxy_setup = request_response_object.get_proxy_setup()
            domain_name = request_response_object.get_domain()
            page_type = request_response_object.get_page_type()
            validation = request_response_object.get_validation_test()
            geo = request_response_object.get_geo()
            custom_tag = request_response_object.get_custom_tag()
        else:
            method = 'no_response'
            status_code = 'no_response'
            proxy_name = 'no_response'
            proxy_setup = 'no_response'
            domain_name = 'no_response'
            page_type = 'no_response'
            validation = 'no_response'
            geo = 'no_response'
            custom_tag = 'no_response'
        custom_signal = 'none'
        self.check_periodic_stats()

        if signal == 'item_scraped':
            self.inc_value(PeriodicStatsModel._periodic_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|items')
            self.inc_value(OverallStatsModel._overall_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|items')
        
        elif signal == 'item_passed':
            self.inc_value(PeriodicStatsModel._periodic_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|items_passed')
            self.inc_value(OverallStatsModel._overall_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|items_passed')
        
        elif signal == 'item_dropped':
            self.inc_value(PeriodicStatsModel._periodic_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|items_dropped')
            self.inc_value(OverallStatsModel._overall_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|items_dropped')
        
        elif signal == 'item_error':
            self.inc_value(PeriodicStatsModel._periodic_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|item_errors')
            self.inc_value(OverallStatsModel._overall_stats, f'responses|{method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{status_code}|{validation}|{geo}|{custom_tag}|{custom_signal}|item_errors')

    

    def generate_exception_stats(self, request_response_object, request=None, exception_class=None):
        proxy_name = request_response_object.get_proxy_name()
        proxy_setup = request_response_object.get_proxy_setup()
        domain_name = request_response_object.get_domain()
        page_type = request_response_object.get_page_type()
        validation = request_response_object.get_validation_test()
        geo = request_response_object.get_geo()
        custom_tag = request_response_object.get_custom_tag()
        custom_signal = 'none'
        exception_type = ExceptionNormalizer.normalise_exception(exception_class)
        download_latency = request.meta.get('download_latency', 0)
        if download_latency is None:
            start_time = request.meta.get('sops_time', 0) 
            if start_time != 0: download_latency = utils.current_time() - start_time
            else: download_latency = 0  

        self.check_periodic_stats()
        self.inc_value(PeriodicStatsModel._periodic_stats, f'responses|{request.method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{exception_type}|{validation}|{geo}|{custom_tag}|{custom_signal}|count')
        self.inc_value(OverallStatsModel._overall_stats, f'responses|{request.method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{exception_type}|{validation}|{geo}|{custom_tag}|{custom_signal}|count')
        self.inc_value(OverallStatsModel._overall_stats, f'responses|{request.method}|{proxy_name}|{proxy_setup}|{domain_name}|{page_type}|{exception_type}|{validation}|{geo}|{custom_tag}|{custom_signal}|total_latency', count=download_latency)
    
    
    def aggregate_stats(self, crawler=None, middleware=False):
        self.avg_latency()
        #TODO: FIX THIS
        #if middleware is False:
            # self.get_exception_stats(crawler)


    def avg_latency(self):
        for stat_type in [PeriodicStatsModel._periodic_stats, self._overall_stats]:
            stats_copy = copy.deepcopy(stat_type)
            for key, value in stats_copy.items():
                if 'responses' in key and 'total_latency' in key:
                    count_key = key.replace('total_latency', 'count')
                    avg_latency = value / stats_copy.get(count_key)
                    self.set_value(stat_type, key.replace('total_latency', 'avg_latency'), avg_latency)



    def exception_type_check(self, key):
        if isinstance(key, str):
            return key.startswith('downloader/exception_type_count/')
        return False
    
    def get_exception_stats(self, crawler):
        scrapy_stats = crawler.stats.get_stats()
        if scrapy_stats.get('downloader/exception_count') is not None:
            exception_values = [ {k:v} for k,v in scrapy_stats.items() if self.exception_type_check(k)]
            
            for exception in exception_values:
                for key, value in exception.items():
                    key_type = key.replace('downloader/exception_type_count/', '')
                    try:
                        exception_type = key_type.split('.')[-1]
                    except Exception:
                        exception_type = key_type
                    self.set_value(self._overall_stats, f'responses|unknown|unknown|unknown|unknown|unknown|{exception_type}|unknown|unknown|unknown|unknown|count', value)



    

            

    
                


    








    
        


