import sys
from dataclasses import dataclass
import numpy as np
from sklearn.pipeline import Pipeline
import os

from src.Exception import CustomException
from src.Logger import logging


class DataTransformationConfig:
    preprocessor_obj_file_path = os.path.join("artifacts", "preprocessor.pkl")


class DataTransformation:
    def __init__(self):
        self.data_transformation_config = DataTransformationConfig()

    def get_data_transformation_object(self):
        try:
            pass
        except:
            pass
