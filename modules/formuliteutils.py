'''
Wrapper for FORMUlite utilities.
'''
from importlib import util
import sys
from .clauses import *

class utils:
    '''
    A wrapper for some utilities provided in this module
    Should not be initialized, all methods are static
    '''
    @staticmethod
    def where(compose=False, separator=" AND ", **kargs):
        return Where(compose, separator, **kargs)

    @staticmethod
    def limit(number):
        return Limit(number)

    @staticmethod
    def load_module(module_name, module_path, piece=None):
        '''Function to import modules dinamically'''
        # print(f"Loading module {module_name}")
        spec = util.spec_from_file_location(module_name, module_path)
        module = util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        if piece:
            return getattr(module, piece)
