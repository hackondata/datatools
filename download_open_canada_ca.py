import json
import urllib.request
import re
import os
from collections import Counter


# *** CHANGE THIS as appropriate ***:
# Base path to save all data to

my_base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/open.canada.ca")

# List of data formats that we don't want to download
skip_download = []
# skip_download = ['HTML','CSV','PDF','XLS','ZIP','XML','TXT','GML']

# INPUT: URL to json file
# OUTPUT: json data
def get_jsonparsed_data(json_url):
    response = urllib.request.urlopen(json_url)
    str_response = response.read().decode('utf-8')
    return json.loads(str_response)


# INPUT: Name of file with URLs. Finds URLs that point to open.canada.
# OUTPUT: URLs to json files, the corresponding open.canada web pages, and open.canada IDs.
def get_json_urls(hyperlinks_text_file_name):
    f = open(hyperlinks_text_file_name, 'r')
    json_urls = [];  # list of URLs to json metadata files of open.canada data
    open_canada_urls = [];  # corresponding list of URLs to open.canada data
    open_canada_IDs = [];  # corresponding IDs
    for line in f:
        #         print(line,end='')
        match = re.search('open.canada.ca', line)  # match = re.search(pat, text)
        if match:
            #             print(line,end='')
            ID = re.findall('/dataset/(.+)', line)
            #             print(ID)
            json_urls.append("http://open.canada.ca/data/api/action/package_show?id=" + str(ID[0]))
            open_canada_urls.append(line.strip('\n'))
            open_canada_IDs.append(ID[0])
    f.close()
    return (json_urls, open_canada_urls, open_canada_IDs)


# INPUT: json description provided by open.canada (and a URL to open.canada web-page)
# OUTPUT: metadata in our format
def parse_orig_json(json_data, open_canada_url):
    my_metadata = {}  # create empty dict to be filled with metadata
    my_metadata['title'] = json_data['result']['title']
    my_metadata['source_page'] = open_canada_url

    # fields below still need to be filled with actual values
    my_metadata['source_files'] = [] # [d.get('url') for d in json_data['result']['resources'] if d.get('url')]  # ['http://url_to_source_file_1','http://url_to_source_file_2']
    my_metadata['Category'] = 'Open Data'
    my_metadata['data_last_modified'] = json_data['result']['revision_timestamp']
    my_metadata['data_schema'] = {}
    my_metadata['description'] = json_data['result']['notes']
    my_metadata['license'] = json_data['result']['license_url']
    my_metadata['tags'] = []
    my_metadata['update_frequency'] = 'Other'
    return my_metadata


# Saves file from URL to folder_name, using specified file_name or automatically assigned one.
# INPUT: URL; folder_name where file will be saved; file_name = 0 for automatic assignment.
def download_file(URL, folder_name, file_name=0):
    if file_name == 0:  # if file name is not specified
        file_name = os.path.basename(URL)  # get file name
    full_path_to_save = os.path.join(folder_name, file_name)
    try:
        urllib.request.urlretrieve(URL, full_path_to_save)
    except urllib.request.HTTPError:  # If unable to download, save failed URL to download_errors.txt
        print('There was an error with the request')
        f = open(os.path.join(folder_name, 'download_errors.txt'), 'a')
        f.write(URL + '\n')
        f.close()


def get_all_data_types(open_canada_IDs, json_urls):
    # Find all types of data resouces, count number of files of each type and get the following result:
    #         {'CSV': 466,
    #          'HTML': 211,
    #          'JSON': 3,
    #          'PDF': 27,
    #          'SHAPE': 3,
    #          'TXT': 18,
    #          'XLS': 111,
    #          'XML': 92,
    #          'ZIP': 38,
    #          'doc': 3,
    #          'fgdb / gdb': 1,
    #          'gml': 3,
    #          'jpeg 2000': 19,
    #          'kml / kmz': 1,
    #          'other': 54,
    #          'rtf': 2,
    #          'wfs': 1,
    #          'wms': 1})
    # Can list these types in skip_download = [] to skip downloading certain types.

    res_type = [];
    for idx in range(0, len(open_canada_IDs)):
        print("Processing data source " + str(idx) + ", ID: " + str(open_canada_IDs[idx]))
        json_data = get_jsonparsed_data(json_urls[idx])
        for res in json_data['result']['resources']:
            res_type.append(res['format'])

    set(res_type)
    res_type.sort()
    return Counter(res_type)

# Get json_urls, open_canada_urls and open_canada_IDs from the text file containing hyperlinks.
( json_urls , open_canada_urls, open_canada_IDs ) = get_json_urls("sources/open.canada.ca.txt")

# Main loop for downloading data from open.data

# for idx in range(0,1):
for idx in range(0, len(open_canada_IDs)):
    print("\nProcessing data source " + str(idx) + ", ID: " + str(open_canada_IDs[idx]))
    folder_path = os.path.join(my_base_path, open_canada_IDs[idx])
    print(folder_path)

    # create folder to download files to
    if not os.path.exists(folder_path):  os.makedirs(folder_path)

    # download original json
    orig_json_filename = open_canada_IDs[idx] + '.json'
    download_file(json_urls[idx], folder_path, orig_json_filename)

    # get data from original json
    json_data = get_jsonparsed_data(json_urls[idx])

    # create metadata from original json
    metadata = parse_orig_json(json_data, open_canada_urls[idx])

    # download all data resources
    for res in json_data['result']['resources']:
        if res['format'] in skip_download:
            print("  Skipping: " + res['url'])
        else:
            print("  Downloading: " + res['url'])
            download_file(res['url'], folder_path)
            metadata['source_files'].append(res['url'])

    # save metadata
    fp = open(os.path.join(folder_path, 'metadata.json'), 'w')
    json.dump(metadata, fp)
    fp.close()