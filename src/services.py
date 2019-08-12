"""
Services module with all different services to read or parse the file
"""
import os
import re

from base import BaseService
from pyspark import SparkContext, SparkConf


class ReadService(BaseService):
    def __init__(self, file_path, file_name=None):
        self.file_path = file_path
        self.file_name = file_name
        self.spark_obj = SparkServices()
        self.inv_obj = InvertedIndexDB()
        self.data = None

    def prepare_files_to_process_list(self):
        files_to_process = []
        if self.file_name:
            files_to_process.append(os.path.join(self.file_path, self.file_name))

        else:
            for dir_, _, files in os.walk(self.file_path):
                for file_name in files:
                    files_to_process.append(os.path.join(self.file_path, file_name))
        self.files_ro_process = files_to_process

        return self.files_ro_process

    def read_file(self, file_path):
        with open(file_path) as f:
            lines = f.readlines()
            # print the list
            for line in lines:
                self.data = self.inv_obj.process_line(line, file_path.split('/')[-1].split('.')[0])
        return self.data

    def print_data_dict(self, data_dict):

        for key, value in sorted(data_dict.items(), key=lambda item: item[0]):
            print("%s: %s" % (key, value))


class InvertedIndexDB(object):

    def __init__(self):
        self.data_db = {}
        self.doc_dict = {}
        self.word_dict = {}
        self.latest_word_id = 0
        self.latest_doc_id = 0

    def get_doc_id(self):
        self.latest_doc_id += 1
        return self.latest_doc_id

    def get_word_id(self):
        self.latest_word_id += 1
        return self.latest_word_id

    def add_word(self, word):

        self.word_dict[word] = self.get_word_id()

    def add_doc(self, doc):
        self.doc_dict.append((self.get_doc_id()), doc)

    def process_line(self, line, doc_id):
        line = re.sub(r'[^\w\s]', '', line)
        word_list = line.split(' ')
        for word in set(word_list):
            word = word.strip()
            if not word:
                continue
            if word not in self.word_dict:
                self.add_word(word)
            word_id = str(self.word_dict[word])

            if word_id not in self.data_db:
                self.data_db[word_id] = [doc_id]
            else:
                l = self.data_db[word_id]
                l.append(doc_id)
                self.data_db[word_id] = list(set(l))

        return self.data_db


class SparkServices(BaseService):
    """
    Spark Service class for creating spark job
    """

    def __init__(self, job_name='BalaSpark'):
        self.job_name = job_name
        self.update_config()

    def update_config(self):
        # create Spark context with Spark configuration
        conf = SparkConf().setAppName(self.job_name)
        self.sc = SparkContext(conf=conf)

    @property
    def service(self):
        return self.sc
