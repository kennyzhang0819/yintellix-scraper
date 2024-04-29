from scrapeops_python_logger.utils import utils
from scrapeops_python_logger.core.api import SOPSRequest
from scrapeops_python_logger.normalizer.middleware import RequestResponseMiddleware  
from scrapeops_python_logger.validators.item_validator import ItemValidator   
from scrapeops_python_logger.core.model import SDKData
from scrapeops_python_logger.core.error_logger import ErrorLogger



class SDKSetup(SDKData):

    def __init__(self):
        SDKData.__init__(self)


    def initialize_SDK(self):
        self.job_args = utils.get_args()
        self.job_group_name = self.get_job_name()
        self.job_group_type = self.check_scrapeops_triggered_job()

        ## System Settings
        self._scrapeops_sdk_version = utils.get_scrapeops_version()
        self._scrapeops_python_version = utils.get_python_version()
        self._scrapeops_system_version = utils.get_system_version()
        self.get_server_details()

        ## Middlewares
        self.initialize_middlewares()
        self.initialize_error_logger()

        SOPSRequest.SCRAPEOPS_LOGGING_DATA = {'logging_data': self.logging_data()}

    

    def initialize_middlewares(self):
        if self.item_validation_middleware is None: 
            self.item_validation_middleware = ItemValidator()



    def initialize_error_logger(self):
        self._error_logger = ErrorLogger(
                        self.spider_name, 
                        self.server_hostname, 
                        self.server_ip,
                        self.start_time,
                        self.log_file)
        

    def initialize_job_details(self, data):
        self.job_id = data.get('job_id')
        self.job_group_name = data.get('job_group_name', self.job_group_name)
        self.job_group_id = SOPSRequest.JOB_GROUP_ID = data.get('job_group_id')
        self.spider_id= data.get('spider_id')
        self.server_id= data.get('server_id')
        self.project_id= data.get('project_id')
        self.multi_server = data.get('multi_server', False)
        SOPSRequest.HIGH_FREQ_ACC = data.get('high_freq', False)
        self._period_frequency = data.get('stats_period_frequency')
        self._period_freq_list = data.get('stats_period_freq_list')
        self.update_sdk_settings(data)
        self.initialize_normalizer_middleware(data)
        SOPSRequest.SCRAPEOPS_LOGGING_DATA = {'logging_data': self.logging_data()}
        

    def initialize_normalizer_middleware(self, data=None):
        if data is not None:
            self._proxy_apis = data.get('proxy_apis', {})
            self._generic_validators = data.get('generic_validators', [])
        if self.request_response_middleware is None:
            self.request_response_middleware = RequestResponseMiddleware(self.job_group_id, 
                                                                            self._proxy_apis, 
                                                                            self._generic_validators, 
                                                                            self._error_logger,
                                                                            self.allowed_response_codes)


    def update_sdk_settings(self, data):
        self._sdk_active = data.get('sdk_active', self._sdk_active) 
        self.multi_server = data.get('multi_server', self.multi_server)

        ## SOPS API Endpoints
        SOPSRequest.set_sops_endpoint(data.get('scrapeops_endpoint', SOPSRequest.SCRAPEOPS_ENDPOINT) )
        SOPSRequest.set_sops_api_version(data.get('scrapeops_api_version')) 

        ## Normalisation Middleware
        RequestResponseMiddleware.PROXY_DOMAIN_NORMALIZATION = data.get('proxy_domain_normalization', RequestResponseMiddleware.PROXY_DOMAIN_NORMALIZATION) 
        RequestResponseMiddleware.PROXY_ALERTS = data.get('proxy_alerts', RequestResponseMiddleware.PROXY_ALERTS)
        RequestResponseMiddleware.RESPONSE_VALIDATION = data.get('response_validation', RequestResponseMiddleware.RESPONSE_VALIDATION)

        # ## Item Validation Middleware
        ItemValidator.ITEM_COVERAGE_ENABLED = data.get('item_coverage_enabled', ItemValidator.ITEM_COVERAGE_ENABLED) 
        ItemValidator.INVALID_ITEM_URLS_LOGGING_ENABLED = data.get('ivalid_item_coverage_url_logging_enabled', ItemValidator.INVALID_ITEM_URLS_LOGGING_ENABLED) 
        ItemValidator.MAX_ITEM_URLS = data.get('max_item_urls', ItemValidator.MAX_ITEM_URLS) 




    

    


    







    

    
         
    