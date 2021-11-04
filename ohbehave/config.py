"""Config"""
import os.path

APP_ROOT = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.join(APP_ROOT, '..')
ENV_DIR = os.path.join(PROJECT_ROOT, 'env')
CACHE_DIR = os.path.join(APP_ROOT, 'data', 'cache')

