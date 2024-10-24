import logging

class ExcludeMessageFilter(logging.Filter):
    """
    Filters out log records that contain any of the specified excluded messages.
    """

    def __init__(self, exclude_messages):
        super().__init__()
        self.exclude_messages = exclude_messages

    def filter(self, record):
        return not any(ex_msg in record.getMessage() for ex_msg in self.exclude_messages)