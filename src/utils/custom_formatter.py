import logging

class CustomFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'topic'):
            record.topic = 'N/A'
        if not hasattr(record, 'run_id'):
            record.run_id = 'N/A'
        if not hasattr(record, 'logger_name'):
            record.logger_name = record.name
        return super(CustomFormatter, self).format(record)