# import csv
# import logging
# from pymongo import MongoClient
# import re
# import time

# class LogParser:
#     def __init__(self):
#         # MongoDB connection parameters
#         self.mongo_client = MongoClient('localhost', 27017)
#         self.mongo_db = self.mongo_client['Logs_DataBase']
#         self.mongo_collection = self.mongo_db['Logs']

#     def parse_and_store(self, csv_filename):
#         with open(csv_filename, 'r', encoding='utf-8') as csvfile:
#             csv_reader = csv.reader(csvfile)
#             for row in csv_reader:
#                 log_document = self.create_log_document(row)
#                 self.insert_into_mongodb(log_document)
            

#     def create_log_document(self, row):

#         #Regular Expression pattern
#         pattern = re.compile(r'(?P<ip_address>\d+\.\d+\.\d+\.\d+) <(?P<severity>\d+)>(?P<timestamp>\S+ \d+ \d{2}:\d{2}:\d{2}) (?P<hostname>\S+) TO-SYSLOG-SERVER: Info: (?P<info_timestamp>\d+\.\d+) (?P<some_info>\d+) (?P<source_ip>\d+\.\d+\.\d+\.\d+) (?P<status>\S+/\d+) (?P<response_time>\d+) (?P<action>\S+) (?P<url>\S+) - (?P<direct_or_other>\S+)/(?P<direct_hostname>\S+) - (?P<category_1>\S+)-(?P<category_2>\S+)-(?P<category_3>\S+)-(?P<category_4>\S+)-(?P<category_5>\S+) <(?P<tags>.+)> - (?P<some_number>\d+)')

#         matches = pattern.match(''.join(row))

#         # Print matches for debugging
#         print(f"Matches: {matches}")

#         if matches:
#             log_document = {
#                 'ip_addr': matches.group('ip_address'),
#                 'severity': matches.group('severity'),
#                 'timestamp': matches.group('timestamp'),
#                 'hostname': matches.group('hostname'),
#                 'info timestamp': matches.group('info_timestamp'),
#                 'some_info': matches.group('some_info'),
#                 'source ip': matches.group('source_ip'),
#                 'response_time': matches.group('response_time'),
#                 'status': matches.group('status'),
#                 'action': matches.group('action'),
#                 'url': matches.group('url'),
#                 'direct/other': matches.group('direct_or_other'),
#                 'direct hostname': matches.group('direct_hostname'),
#                 'tags': matches.group('tags')
#                 # Add other fields as needed
#             }
#         else:
#             logging.warning(f"Failed to parse log data: ")
#             log_document = {
#                 'ip_addr': '',
#                 'severity': '',
#                 'timestamp': '',
#                 'hostname': '',
#                 'info timestamp': '',
#                 'some_info': '',
#                 'source ip': '',
#                 'response_time': '',
#                 'status': '',
#                 'action': '',
#                 'url': '',
#                 'direct/other': '',
#                 'direct hostname': '',
#                 'tags': ''
#                 # Add other fields as needed
#             }

#         return log_document


#     def insert_into_mongodb(self, log_document):
#         # Insert the document into MongoDB
#         self.mongo_collection.insert_one(log_document)
#         logging.info("Inserted log data into MongoDB")

# if __name__ == '__main__':
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#     # Specify the CSV files to parse
#     csv_files = ["window_logs.csv", "window_logs2.csv"]

#     log_parser = LogParser()

#     for csv_file in csv_files:
#         logging.info(f"Parsing and storing data from {csv_file}")
#         log_parser.parse_and_store(csv_file)

#     logging.info("Log parsing and storing completed")



import csv
import logging
from pymongo import MongoClient
import re
import time

class LogParser:
    def __init__(self):
        # MongoDB connection parameters
        self.mongo_client = MongoClient('localhost', 27017)
        self.mongo_db = self.mongo_client['Logs_DataBase']
        self.mongo_collection = self.mongo_db['Logs']

    def parse_and_store(self, csv_filename):
        with open(csv_filename, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            for row in csv_reader:
                log_document = self.create_log_document(row)
                self.insert_into_mongodb(log_document)

    def create_log_document(self, row):
        # Regular Expression pattern
        pattern = re.compile(r'(?P<ip_address>\d+\.\d+\.\d+\.\d+) <(?P<severity>\d+)>(?P<timestamp>\S+ \d+ \d{2}:\d{2}:\d{2}) (?P<hostname>\S+) TO-SYSLOG-SERVER: Info: (?P<info_timestamp>\d+\.\d+) (?P<some_info>\d+) (?P<source_ip>\d+\.\d+\.\d+\.\d+) (?P<status>\S+/\d+) (?P<response_time>\d+) (?P<action>\S+) (?P<url>\S+) - (?P<direct_or_other>\S+)/(?P<direct_hostname>\S+) - (?P<category_1>\S+)-(?P<category_2>\S+)-(?P<category_3>\S+)-(?P<category_4>\S+)-(?P<category_5>\S+) <(?P<tags>.+)> - (?P<some_number>\d+)')

        matches = pattern.match(''.join(row))

        if matches:
            log_document = {
                'ip_addr': matches.group('ip_address'),
                'severity': matches.group('severity'),
                'timestamp': matches.group('timestamp'),
                'hostname': matches.group('hostname'),
                'info timestamp': matches.group('info_timestamp'),
                'some_info': matches.group('some_info'),
                'source ip': matches.group('source_ip'),
                'response_time': matches.group('response_time'),
                'status': matches.group('status'),
                'action': matches.group('action'),
                'url': matches.group('url'),
                'direct/other': matches.group('direct_or_other'),
                'direct hostname': matches.group('direct_hostname'),
                'tags': matches.group('tags')
                # Add other fields as needed
            }
        else:
            logging.warning(f"Failed to parse log data: ")
            log_document = {
                'ip_addr': '',
                'severity': '',
                'timestamp': '',
                'hostname': '',
                'info timestamp': '',
                'some_info': '',
                'source ip': '',
                'response_time': '',
                'status': '',
                'action': '',
                'url': '',
                'direct/other': '',
                'direct hostname': '',
                'tags': ''
                # Add other fields as needed
            }

        return log_document

    def insert_into_mongodb(self, log_document):
        # Insert the document into MongoDB
        self.mongo_collection.insert_one(log_document)
        logging.info("Inserted log data into MongoDB")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    log_parser = LogParser()

    while True:
        # Specify the CSV files to parse
        csv_files = ["window_logs.csv", "window_logs2.csv"]

        for csv_file in csv_files:
            logging.info(f"Parsing and storing data from {csv_file}")
            log_parser.parse_and_store(csv_file)

        logging.info("Log parsing and storing completed")

        # Adjust the delay (in seconds) based on your requirements
        time.sleep(60)  # Wait for 60 seconds before the next iteration
