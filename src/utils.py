#!/usr/bin/env python3

from functools import update_wrapper

def log_function(func):
    def newfunc(*args, **kwargs):
        print("Entering function '{}'".format(func.__name__))
        try:
            return func(*args, **kwargs)
        finally:
            print("Exiting function '{}'".format(func.__name__))
    update_wrapper(newfunc, func)
    return newfunc
