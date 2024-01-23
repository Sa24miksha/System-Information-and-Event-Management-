# import csv
# import logging
# from pymongo import MongoClient
# import re

# class LogParser:
 

#     def parse_and_store(self, csv_filename):
#         with open(csv_filename, 'r', encoding='utf-8') as csvfile:
#             csv_reader = csv.reader(csvfile)
#             for row in csv_reader:
#                 # print(row)
#                 # pattern = re.compile(r'\d')
#                 # pattern = re.compile(r'(?P<ip_address>\d+\.\d+\.\d+\.\d+) <(?P<severity>\d+)>(?P<timestamp>\S+ \d+ \d{2}:\d{2}:\d{2}) (?P<hostname>\S+) TO-SYSLOG-SERVER: Info: (?P<info_timestamp>\d+\.\d+) \d+ (?P<source_ip>\d+\.\d+\.\d+\.\d+) (?P<status>\S+/\d+) \d+ (?P<action>\S+) (?P<url>\S+)')

#                 pattern = re.compile(r'(?P<ip_address>\d+\.\d+\.\d+\.\d+) <(?P<severity>\d+)>(?P<timestamp>\S+ \d+ \d{2}:\d{2}:\d{2}) (?P<hostname>\S+) TO-SYSLOG-SERVER: Info: (?P<info_timestamp>\d+\.\d+) (?P<some_info>\d+) (?P<source_ip>\d+\.\d+\.\d+\.\d+) (?P<status>\S+/\d+) (?P<response_time>\d+) (?P<action>\S+) (?P<url>\S+) - (?P<direct_or_other>\S+)/(?P<direct_hostname>\S+) - (?P<category_1>\S+)-(?P<category_2>\S+)-(?P<category_3>\S+)-(?P<category_4>\S+)-(?P<category_5>\S+) <(?P<tags>.+)> - (?P<some_number>\d+)')

                
#                 match = pattern.match(''.join(row))
#                 if (match):
#                     print ("yes for ") #, ''.join(row))

#                     # PRINT IP ADDRESS HERE
#                     # print ("IP Address: ", match.group('ip_address'))                    
#                     # print ("Hostame: ", match.group('hostname'))                    
#                     # print ("Status: ", match.group('status'))  

#                     print("IP Address:", match.group('ip_address'))
#                     print("Severity:", match.group('severity'))
#                     print("Timestamp:", match.group('timestamp'))
#                     print("Hostname:", match.group('hostname'))
#                     print("Info Timestamp:", match.group('info_timestamp'))
#                     print("Some Info:", match.group('some_info'))
#                     print("Source IP:", match.group('source_ip'))
#                     print("Status:", match.group('status'))
#                     print("Response Time:", match.group('response_time'))
#                     print("Action:", match.group('action'))
#                     print("URL:", match.group('url'))
#                     print("Direct/Other:", match.group('direct_or_other'))
#                     print("Direct Hostname:", match.group('direct_hostname'))
#                     print("Category 1:", match.group('category_1'))
#                     print("Category 2:", match.group('category_2'))
#                     print("Category 3:", match.group('category_3'))
#                     print("Category 4:", match.group('category_4'))
#                     print("Category 5:", match.group('category_5'))
#                     print("Tags:", match.group('tags'))
#                     print("Some Number:", match.group('some_number'))                  
                    
                


#             # print("Header:", csv_reader)
#             #print(csv_reader)
#             print("A")


#             # for row in csv_reader:
#             #     print("N")
#             #     log_document = self.create_log_document(row)
#             #     print(log_document)
#             #     print("DD")
#             # print("B")
    
#     def create_log_document(self, row):
#         print(row)
#         # Parse CSV row and create a document to insert into MongoDB
#         log_data = row.get('log_data', '')
        
#         # Print log_data for debugging
#         print(f"Log Data: {log_data}")

    


# # # Specify the path to your CSV file
# # csv_file_path = 'your_file.csv'

# # # Open the CSV file in read mode
# # with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
# #     # Create a CSV reader object
# #     csv_reader = csv.reader(csvfile)

# #     # Iterate through each row in the CSV file
# #     for row in csv_reader:
# #         # 'row' is a list representing the values in the current row
# #         print(row)







    
# if __name__ == '__main__':
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#     # Specify the CSV files to parse
#     csv_files = ["window_logs.csv"]

#     log_parser = LogParser()

#     for csv_file in csv_files:
#         logging.info(f"Parsing and storing data from {csv_file}")
#         log_parser.parse_and_store(csv_file)
        

#     logging.info("Log parsing and storing completed")





import socket
import logging
from pymongo import MongoClient
import csv
import re
import time
from concurrent.futures import ThreadPoolExecutor

class LogProcessor:
    def __init__(self):
        # MongoDB connection parameters
        self.mongo_client = MongoClient('localhost', 27017)
        self.mongo_db = self.mongo_client['Logs_DataBase']
        self.mongo_collection = self.mongo_db['Logs']
        self.log_parser = LogParser()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0', 514))

        self.log_buffer = {}  # Dictionary to store logs based on IP address

    def process_logs(self, data, addr):
        log_data = (data).decode('utf-8')
        ip_address = addr[0]

        if ip_address not in self.log_buffer:
            self.log_buffer[ip_address] = []

        self.log_buffer[ip_address].append(log_data)

        # Process and store logs
        self.log_parser.parse_and_store(ip_address, self.log_buffer[ip_address])

    def receive_and_process_logs(self):
        with ThreadPoolExecutor() as executor:
            try:
                while True:
                    data, addr = self.sock.recvfrom(65535)
                    executor.submit(self.process_logs, data, addr)
            except Exception as e:
                logging.error(f"Exception: {e}")
                self.cleanup()

    def cleanup(self):
        self.sock.close()
       # sys.exit()

class LogParser:
    def __init__(self):
        # MongoDB connection parameters
        self.mongo_client = MongoClient('localhost', 27017)
        self.mongo_db = self.mongo_client['Logs_DataBase']
        self.mongo_collection = self.mongo_db['Logs']

    def parse_and_store(self, ip_address, log_data_list):
        for log_data in log_data_list:
            row = log_data.split()
            log_document = self.create_log_document(row)

            # Add the IP address to the log document
            log_document['ip_addr'] = ip_address

            self.insert_into_mongodb(log_document)

    def create_log_document(self, row):
        # Regular Expression pattern
        pattern = re.compile(r'(?P<ip_address>\d+\.\d+\.\d+\.\d+) <(?P<severity>\d+)>(?P<timestamp>\S+ \d+ \d{2}:\d{2}:\d{2}) (?P<hostname>\S+) TO-SYSLOG-SERVER: Info: (?P<info_timestamp>\d+\.\d+) (?P<some_info>\d+) (?P<source_ip>\d+\.\d+\.\d+\.\d+) (?P<status>\S+/\d+) (?P<response_time>\d+) (?P<action>\S+) (?P<url>\S+) - (?P<direct_or_other>\S+)/(?P<direct_hostname>\S+) - (?P<category_1>\S+)-(?P<category_2>\S+)-(?P<category_3>\S+)-(?P<category_4>\S+)-(?P<category_5>\S+) <(?P<tags>.+)> - (?P<some_number>\d+)')

        matches = pattern.match(' '.join(row))

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

    log_processor = LogProcessor()
    log_processor.receive_and_process_logs()
