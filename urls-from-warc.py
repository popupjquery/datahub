import os
import re
import concurrent.futures
from warcio.archiveiterator import ArchiveIterator

def extract_hosts_from_warc(file_path):
    hosts = []
    with open(file_path, 'rb') as warc_file:
        for record in ArchiveIterator(warc_file):
            if record.rec_type == 'response':
                http_headers = record.http_headers
                host = http_headers.get_header('Host')
                if host:
                    hosts.append(host)
    return hosts

def process_warc_file(file_path, result_folder):
    try:
        file_name = os.path.basename(file_path)
        hosts = extract_hosts_from_warc(file_path)
        
        if hosts:
            result_path = os.path.join(result_folder, f'hosts_{file_name}.txt')
            with open(result_path, 'w') as result_file:
                result_file.write('\n'.join(hosts))

        os.remove(file_path)  # Удаление оригинального варк-файла

        return f"Processed: {file_name}"
    except Exception as e:
        return f"Error processing {file_name}: {e}"

def main():
    warc_folder = 'D:\\zones\\warc'
    result_folder = 'D:\zones\\result'

    if not os.path.exists(result_folder):
        os.makedirs(result_folder)

    warc_files = [os.path.join(warc_folder, file) for file in os.listdir(warc_folder) if file.endswith('.warc')]

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(process_warc_file, file_path, result_folder) for file_path in warc_files]

        for future in concurrent.futures.as_completed(futures):
            print(future.result())

if __name__ == "__main__":
    main()
