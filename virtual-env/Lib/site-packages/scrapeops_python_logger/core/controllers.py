

from scrapeops_python_logger.utils import utils
from scrapeops_python_logger.core.setup import SDKSetup
from scrapeops_python_logger.core.api import SOPSRequest 
import sys

class SDKControllers(SDKSetup):

    SETUP_ATTEMPT_LIMIT = 3

    def __init__(self):
        SDKSetup.__init__(self)
    

    def check_api_key_present(self):
        if self._scrapeops_api_key == None:
            self._sdk_active = False
            return False
        self._sdk_active = True
        return True
    

    def send_setup_request(self):
        data, status = SOPSRequest().setup_request(body=self.setup_data())
        if status.valid:
            self._scrapeops_api_key_valid = True
            self.initialize_job_details(data)
        elif status.action == 'retry' and self._setup_attempts < SDKControllers.SETUP_ATTEMPT_LIMIT:
            self._setup_attempts += 1
            self._error_logger.log_error(reason='setup_failed', 
                                         error=status.error, 
                                         data={'setup_attempts': self._setup_attempts})
        elif status.action == 'retry' and self._setup_attempts >= SDKControllers.SETUP_ATTEMPT_LIMIT:
            self.deactivate_sdk(reason='exceeded_max_setup_attempts', 
                                error=status.error, 
                                data={'setup_attempts': self._setup_attempts}, 
                                request_type='setup')
        else:
            self.deactivate_sdk(reason=status.error, data=data, request_type='setup')


    def send_stats(self, periodic_stats=None, overall_stats=None, reason=None, stats_type=None):
        self._sdk_run_time = self._sdk_run_time + self._period_frequency 
        post_body = self.stats_data(periodic_stats=periodic_stats, overall_stats=overall_stats, stats_type=stats_type, reason=reason)

        if self.job_active() is False:
            self.send_setup_request()

        ## retest if job is inactive
        if self.job_active() is False:
            self.failed_periods += 1
            self.cache_failed_stats(post_body)
            self._error_logger.log_error(reason=f'sending_{stats_type}_stats_failure', 
                                        data={'failed_periods': self.failed_periods})

        if self.job_active():
            if stats_type == 'finished' and self.export_logs():
                log_body = self.log_data()
                with open(self.log_file, 'rb') as f:
                    data, status = SOPSRequest().stats_request(body=post_body, log_body=log_body, files={'file': f}) 
            else:
                data, status = SOPSRequest().stats_request(body=post_body) 
            
            if status.valid:
                self.update_sdk_settings(data) 
                self.reset_failed_stats()
            elif status.action == 'retry' and self.failed_periods < 3:
                self.failed_periods += 1
                self.cache_failed_stats(post_body)
                self._error_logger.log_error(reason=f'sending_{stats_type}_stats_failure', 
                                            error=status.error, 
                                            data={'failed_periods': self.failed_periods})
            else:
                self.deactivate_sdk(reason=f'sending_{stats_type}_stats_failure', error=status.error,
                                    data={'failed_periods': self.failed_periods}, request_type=stats_type)  


    def job_active(self):
        if self.job_id is None and self._sdk_active:
            return False
        return True

    
    def export_logs(self):
        # if self._scrapeops_export_scrapy_logs and self.log_file is not None:
        #     return True
        return False



    def deactivate_sdk(self, reason=None, error=None, request_type=None, data=None):
        self._sdk_active = False

        deactivatedMessage = 'Scrapy SDK deactivated'
        print(deactivatedMessage)
        if reason != 'scrapy_shell':
            self._error_logger.sdk_error_close(reason=reason, error=error, request_type=request_type)


    def reset_failed_stats(self):
        self.cached_failed_stats = []
        self.failed_periods = 0

    def cache_failed_stats(self, post_body):
        self.cached_failed_stats.append(post_body)
        self.failed_periods = len(self.cached_failed_stats)




#### FUNCTIONS TO FIX/ADDED ####

# self._error_logger.sdk_error_close
# self._error_logger.log_error


#### VARIABLES TO FIX/ADD ####

#self.cached_failed_stats = []
#self.failed_periods = 0