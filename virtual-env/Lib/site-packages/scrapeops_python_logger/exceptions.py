

class ScrapeOpsMissingAPIKey(Exception):
    """Indicates that no ScrapeOps API key added"""
    def __init__(self):
        self.message = 'No ScrapeOps API key defined.'
        super().__init__(self.message)
    
    def __str__(self):
        return f'ScrapeOpsMissingAPIKey: {self.message}'


class ScrapeOpsInvalidAPIKey(Exception):
    """Indicates that the API key is invalid"""
    def __init__(self):
        self.message = 'ScrapeOps API key is invalid'
        super().__init__(self.message)
    
    def __str__(self):
        return f'ScrapeOpsInvalidAPIKey: {self.message}'


class ScrapeOpsAPIResponseError(Exception):
    
    def __init__(self):
        super().__init__()


class DecodeError(Exception):
    pass