import logging
import inspect
import os


class Logger:
    def __init__(self, logger_name='Logger', log_level=logging.DEBUG, log_file=None):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(class_name)s - %(method_name)s - %(message)s')
                
        # Create file handler if log_file is provided
        if log_file:            
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(log_level)
            file_handler.setFormatter(self.formatter)
            self.logger.addHandler(file_handler)
        else:
            # Create console handler and set level to log_level
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            console_handler.setFormatter(self.formatter)
            self.logger.addHandler(console_handler)
            

    def info(self, message, class_name=None, method_name=None):
        class_name = class_name or self._get_calling_class_name()
        method_name = method_name or self._get_calling_method_name()
        self.logger.info(message, extra={'class_name': class_name, 'method_name': method_name})

    def warning(self, message, class_name=None, method_name=None):
        class_name = class_name or self._get_calling_class_name()
        method_name = method_name or self._get_calling_method_name()
        self.logger.warning(message, extra={'class_name': class_name, 'method_name': method_name})

    def error(self, message, class_name=None, method_name=None):
        class_name = class_name or self._get_calling_class_name()
        method_name = method_name or self._get_calling_method_name()
        self.logger.error(message, extra={'class_name': class_name, 'method_name': method_name})

    def exception(self, message, class_name=None, method_name=None):
        class_name = class_name or self._get_calling_class_name()
        method_name = method_name or self._get_calling_method_name()
        self.logger.exception(message, extra={'class_name': class_name, 'method_name': method_name})

    def debug(self, message, class_name=None, method_name=None):
        class_name = class_name or self._get_calling_class_name()
        method_name = method_name or self._get_calling_method_name()
        self.logger.debug(message, extra={'class_name': class_name, 'method_name': method_name})

    def _get_calling_class_name(self):        
        stack = inspect.stack()
        calling_class_name = stack[2].frame.f_locals.get('self').__class__.__name__
        return calling_class_name

    def _get_calling_method_name(self):        
        stack = inspect.stack()
        calling_method_name = stack[2].function
        return calling_method_name
