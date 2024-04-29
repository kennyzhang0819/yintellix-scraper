import logging
import threading
import time
from scrapeops_python_logger.core.controllers import SDKControllers
from scrapeops_python_logger.core.error_logger import TailLogger
from scrapeops_python_logger.stats.logger import StatsLogger
from scrapeops_python_logger.normalizer.request_response import SOPSResponse
from scrapeops_python_logger.stats.model import OverallStatsModel, PeriodicStatsModel
from scrapeops_python_logger.exceptions import ScrapeOpsMissingAPIKey, ScrapeOpsInvalidAPIKey
from scrapeops_python_logger.utils import utils
from scrapeops_python_logger.core.api import SOPSRequest

import atexit

class ScrapeOpsLogger(SDKControllers, StatsLogger):

    def __init__(self, 
        scrapeops_api_key=None, 
        spider_name=None, 
        job_name=None, 
        job_version=None,
        custom_groups=None,
        logger_name=None,

        ## SOPS
        sop_debug=False, 
        sops_endpoint=None):

        SDKControllers.__init__(self)
        StatsLogger.__init__(self)

        ## Error/Warning Logger
        self.tail = TailLogger()
        log_handler = self.tail.log_handler

        if(logger_name != None):
            logging.getLogger(logger_name).addHandler(log_handler)
        else:
            logging.getLogger().addHandler(log_handler)


        #Periodic Details
        self.daemon_thread = None
        self.last_run = 0
        self.thread_active = True
        self.sleep_frequency = 0.5


        ## Job Details
        self.spider_name = spider_name
        self.job_group_name = job_name
        self.job_group_version = job_version
        self.job_custom_groups = custom_groups

        ## Logger Setup Data
        self._scrapeops_api_key = scrapeops_api_key
        self._scrapeops_debug_mode = sop_debug
        SOPSRequest.set_sops_endpoint(sops_endpoint)
        SOPSRequest.set_sops_api_key(scrapeops_api_key) 

        self.start_sdk()
        


    def start_sdk(self, spider=None, crawler=None):
        self.start_time = self.period_start_time = utils.current_time()
        self._scrapeops_job_start = utils.current_time()

        if self.check_api_key_present():
            self.initialize_SDK()
            self.send_setup_request()
            if self._scrapeops_api_key_valid:
                self.spider_open_stats()
                self.start_periodic_monitor()
                atexit.register(self.close_sdk)
            else:
                print("ScrapeOps API Key Invalid")
                err = ScrapeOpsInvalidAPIKey()
                self.deactivate_sdk(reason='invalid_api_key', error=err)
        else:
            print("ScrapeOps API Key Missing or Incorrect")
            err = ScrapeOpsMissingAPIKey()
            self.deactivate_sdk(reason='no_api_key', error=err)
            raise err
            

    #PERIODIC 1st function
    def start_periodic_monitor(self):
        self.daemon_thread = threading.Thread(target=self.scheduler_controller, daemon=True)
        self.daemon_thread.start()
    
    #PERIODIC 2nd function
    def scheduler_controller(self): 
        while self.thread_active == True:
            time.sleep(self.sleep_frequency)
            self.last_run += self.sleep_frequency

            #send stats
            period_time = utils.current_time()
            if self.get_runtime(time=period_time) % self.get_periodic_frequency() == 0:
                self.period_finish_time = utils.current_time()

                if self.sdk_enabled():
                    self.spider_close_stats()
                    self.aggregate_stats(crawler=None, middleware=False) 
                    self.send_stats(periodic_stats=PeriodicStatsModel._periodic_stats, overall_stats=OverallStatsModel._overall_stats, stats_type='periodic', reason='periodic') 
                    self.reset_periodic_stats()
                    self.period_start_time = utils.current_time()
                    self.inc_value(OverallStatsModel._overall_stats, 'periodic_runs') 
                elif self.periodic_monitor_active(): 
                    self.close_periodic_monitor()

    def close_periodic_monitor(self):
        self.thread_active = False



    def get_periodic_frequency(self):
        self.period_count = 0
        runtime = self.get_runtime()
        if self._period_freq_list is None:
            self.period_count = int(runtime//self._period_frequency)
            return self._period_frequency
        for index, row in enumerate(self._period_freq_list):
            if runtime > int(row.get('total_time')):
                if index == 0:
                    period_time = row.get('total_time') 
                else:
                    period_time = row.get('total_time') - self._period_freq_list[index - 1].get('total_time') 
                self.period_count += int(period_time/row.get('periodic_frequency'))
            if runtime <= int(row.get('total_time')):
                self._period_frequency = row.get('periodic_frequency')
                if index == 0:
                    diff = runtime
                else:
                    diff = runtime - int(self._period_freq_list[index - 1].get('total_time'))
                self.period_count += int(diff//self._period_frequency)
                return self._period_frequency 

        return self._period_frequency


    def get_runtime(self, time=None):

        if time is None:
            return utils.current_time() - self._scrapeops_job_start 
        return time - self._scrapeops_job_start 

        
    def close_sdk(self):
        if self.sdk_enabled():
            self.period_finish_time = utils.current_time()
            self.spider_close_stats("Finished")
            self.send_stats(periodic_stats=PeriodicStatsModel._periodic_stats, overall_stats=OverallStatsModel._overall_stats, stats_type='finished', reason='finished') 
            self.close_periodic_monitor()
        self.display_overall_stats()



    def log_request(self, request_response_object=None):
        if self.sdk_enabled():
            self.request_response_middleware.normalise_domain_proxy_data(request_response_object)
            self.generate_request_stats(request_response_object) 


    def log_response(self, request_response_object=None):
        if self.sdk_enabled():
            self.request_response_middleware.process(request_response_object) 
            self.generate_response_stats(request_response_object)
            
    
    def item_scraped(self, response=None, item=None): 
        if self.sdk_enabled():
            if isinstance(response, SOPSResponse):
                self.request_response_middleware.normalise_domain_proxy_data(response) 
            self.item_validation_middleware.validate(response, item)
            self.generate_item_stats(response, signal='item_scraped')

    
    def item_dropped(self, response=None, item=None, message=None): 
        if self.sdk_enabled():
            if isinstance(response, SOPSResponse):
                self.request_response_middleware.normalise_domain_proxy_data(response) 
            self.generate_item_stats(response, signal='item_dropped')
            ## log the dropped item


    def item_error(self, response=None, item=None, message=None, error=None): 
        if self.sdk_enabled():
            if isinstance(response, SOPSResponse):
                self.request_response_middleware.normalise_domain_proxy_data(response) 
            self.generate_item_stats(response, signal='item_error')
            ## log the item error
    

    def sdk_enabled(self):
        return self._sdk_active or False

    
    
