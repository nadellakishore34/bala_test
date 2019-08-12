"""
A Module with a job to read some data from pyspark job.
"""

import sys
import re

from src.services import ReadService

DATA_DICT = {}


if __name__ == "__main__":
    global DATA_DICT
    srvc_obj = ReadService('/Users/venkatareddymulam/Desktop/BalaAssigment/dataset')
    data_files = srvc_obj.prepare_files_to_process_list()
    for file_nm in data_files:
        DATA_DICT.update(srvc_obj.read_file(file_nm))

    srvc_obj.print_data_dict(DATA_DICT)


    # mystring1 = """   Project Gutenberg's Etext of Shakespeare's First Folio/35 Play  """
    # inv_obj = InvertedIndexDB()
    # inv_obj.process_line(mystring1, 1)
    #
    # mystring2 = """   Project  First Folio/35 Play  """
    # # obj = inverted_index_db()
    # inv_obj.process_line(mystring2, 2)
    # a=0