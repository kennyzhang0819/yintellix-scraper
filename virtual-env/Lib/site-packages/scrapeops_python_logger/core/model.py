import socket
from scrapeops_python_logger.core.api import SOPSRequest


class BaseSDKModel(object):

    """
        SDK Model:
        The core data types used to control the SDK's operation. 
    """
    
    def __init__(self):
        ## User Data
        self._scrapeops_api_key = None

        ## SDK Data
        self._sdk_active = None
        self._scrapeops_api_key_valid = False
        self._scrapeops_endpoint = None
        self._period_frequency = 60 
        self._period_freq_list = None
        self._sdk_run_time = 0
        self._setup_attempts = 0
        self._scrapeops_test_id = None
        self._error_logger = None
        self._scrapeops_sdk_version = None
        self._scrapeops_python_version = None
        self._scrapeops_system_version = None
        self._scrapeops_job_start = None
        
        ## Spider Data
        self.spider_name = None
        self.retry_enabled = None
        self.retry_times = None
        self.log_file = None

        ## Overall Job Data
        self.job_args = None
        self.job_id = None
        self.job_group_id = None
        self.job_group_uuid = None
        self.job_group_name = None
        self.job_group_version = None
        self.job_custom_groups = None
        self.start_time = None
        self.finish_time = None
        self.server_hostname = None
        self.server_ip = None
        self._proxy_apis  = {}
        self._generic_validators = {}
        self.multi_server = False
        self.failed_urls = []

        ## Period Data
        self.period_start_time = None
        self.period_finish_time = None
        self.period_run_time = 0
        self.period_concurrency = 0
        self.period_count = 0
        
        ## ScrapeOps Triggered Jobs
        self._scrapeops_server_id = None
        self.job_group_type = None

        ## Periodic Monitor
        self.loop = None
        self.periodic_loop = None

        ## Validation/Normalisation Data
        self.proxy_domains = []

        ## Failure 
        self.failed_periods = 0
        self.cached_failed_stats = []

        ## Middleware
        self.request_response_middleware = None
        self.item_validation_middleware = None
        self.failed_url_middleware = None

        self.allowed_response_codes = []


class SDKData(BaseSDKModel):

    def __init__(self):
        BaseSDKModel.__init__(self)


    def setup_data(self):
        return {
            'sops_api_key': self._scrapeops_api_key,
            'job_group_name': self.job_group_name,
            'job_group_version': self.job_group_version,
            'job_group_identifier': self.job_group_uuid,
            'job_group_type': self.job_group_type, 
            'job_args': self.job_args,
            'job_start_time': self.start_time,
            'sops_sdk': 'python-requests',
            'sops_scrapeops_version': self._scrapeops_sdk_version,
            'sops_python_version': self._scrapeops_python_version,
            'sops_system_version': self._scrapeops_system_version,
            'sops_test_id': self._scrapeops_test_id,
            'sops_server_id': self._scrapeops_server_id,
            'scrapeops_job_start': self._scrapeops_job_start,
            'spider_name': self.spider_name,
            'job_custom_groups': self.job_custom_groups,
            'server_ip': self.server_ip,
            'server_hostname': self.server_hostname,
            'multi_server': self.multi_server,
            'retry_enabled': self.retry_enabled,
            'retry_times': self.retry_times,
        }
    

    def stats_data(self, periodic_stats=None, overall_stats=None, stats_type=None, reason=None):
        data = {
            'job_id': self.job_id,
            'job_group_id': self.job_group_id,
            'type': stats_type,
            'period_start_time': self.period_start_time,
            'period_finish_time': self.period_finish_time,
            'period_run_time': self._period_frequency, 
            'sdk_run_time': self._sdk_run_time,
            'periodic': periodic_stats,
            'overall': overall_stats,
            'cached_failed_stats': self.cached_failed_stats,
            'periodic_warnings': periodic_stats.get('log_count/WARNING', 0),
            'periodic_errors': periodic_stats.get('log_count/ERROR', 0),
            'periodic_criticals': periodic_stats.get('log_count/CRITICAL', 0),
            'overall_warnings': overall_stats.get('log_count/WARNING', 0),
            'overall_errors': overall_stats.get('log_count/ERROR', 0),
            'overall_criticals': overall_stats.get('log_count/CRITICAL', 0),
            'multi_server': self.multi_server,
            'period_count': self.period_count,
            'data_coverage': self.item_validation_middleware.get_item_coverage_data(),
            'invalid_items_count': self.item_validation_middleware.get_num_invalid_items(),
            'field_coverage': self.item_validation_middleware.get_field_coverage(),
            'failed_urls_count': len(self.failed_urls),
            'failed_urls_enabled': True,
            'job_custom_groups': self.job_custom_groups,
            'error_details': self.tail.contents(),
            'error_details_cumulative': self.tail.contents('cumulative'),
            'high_freq': SOPSRequest.HIGH_FREQ_ACC
        }

        if stats_type == 'finished':
            data['job_finish_time'] = self.period_finish_time
            data['job_status'] = stats_type
            data['job_finish_reason'] = reason
        return data
    

    def log_data(self):
        return {
            'job_group_id': self.job_group_id,
            'job_group_name': self.job_group_name,
            'job_group_identifier': self.job_group_uuid,
            'spider_name': self.spider_name,
            'sops_sdk': 'python-requests',
        }

    

    def logging_data(self):
        return {
            'sops_api_key': self._scrapeops_api_key,
            'job_id': self.job_id,
            'job_group_id': self.job_group_id,
            'job_group_identifier': self.job_group_uuid,
            'job_group_name': self.job_group_name,
            'spider_name': self.spider_name,
            'server_ip': self.server_ip,
            'server_hostname': self.server_hostname,
            'sops_scrapeops_version': self._scrapeops_sdk_version,
            'sops_python_version': self._scrapeops_python_version,
            'sops_system_version': self._scrapeops_system_version,
            'sops_sdk': 'python-requests',
        }

    def get_job_name(self):
        if self.job_group_name is not None:
            return self.job_group_name 
        ## need to add check when sent from frontend
        return self.spider_name

    def check_scrapeops_triggered_job(self):
        ## need to add check when sent from frontend
        return 'user_triggered'

    def get_server_details(self):
        try:
            self.server_hostname = socket.gethostname()
            self.server_ip = socket.gethostbyname(self.server_hostname)
        except Exception:
            self.server_hostname = 'unknown'
            self.server_ip = 'unknown'


    



