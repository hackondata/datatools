import json
import urllib.request
import re
import os
from bs4 import BeautifulSoup

# *** CHANGE THIS as appropriate ***:
# Base path to save all data to
my_base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data/toronto.ca")


# INPUT: URL to json file
# OUTPUT: json data
def get_jsonparsed_data(json_url):
    response = urllib.request.urlopen(json_url)
    str_response = response.read().decode('utf-8')
    return json.loads(str_response)


# INPUT: html from toronto.ca
# OUTPUT: metadata in our format
def create_metadata(html):
    #     html = urllib.request.urlopen(toronto_URLs[idx]).read()
    my_metadata = {}  # create empty dict to be filled with metadata
    #     my_metadata['title'] = re.findall(r'<li class="active">(.+?)</li>\\r\\n\\r\\n\\t\\t\\t\\t</ol>', str(html))
    soup = BeautifulSoup(html, "lxml")
    my_metadata['title'] = urlify_title(soup.title.string)
    my_metadata['source_page'] = toronto_URLs[idx]
    my_metadata['update_frequency'] = re.findall(r'Refresh rate.+?<dd>(.+?)</dd>', str(html));

    # fields below still need to be filled with actual values
    my_metadata['source_files'] = []  # ['http://url_to_source_file_1','http://url_to_source_file_2']
    my_metadata['data_last_modified'] = ''  # '<2016-06-30>'
    my_metadata['Category'] = 'Open Data'
    my_metadata['data_schema'] = {}
    my_metadata['description'] = 'A description'
    my_metadata['license'] = 'open'
    my_metadata['tags'] = []
    return my_metadata


# Saves file from URL to folder_name, using specified file_name or automatically assigned one.
# INPUT: URL; folder_name where file will be saved; file_name = 0 for automatic assignment; rewrite = 0, overwrite existing file
def download_file(URL, folder_name, file_name=0, rewrite=0):
    if file_name == 0:  # if file name is not specified
        file_name = os.path.basename(URL)  # get file name
    full_path_to_save = os.path.join(folder_name, file_name)
    if not rewrite:  # then check if exists
        if os.path.isfile(full_path_to_save):
            print('File already exists, skipping...')
            return  # don't retrive file if it already exists
    try:  # download
        urllib.request.urlretrieve(URL, full_path_to_save)
    except (
    urllib.request.HTTPError, urllib.request.URLError):  # If unable to download, save failed URL to download_errors.txt
        print('    Download ERROR, skipping...')
        f = open(os.path.join(folder_name, 'download_errors.txt'), 'a')
        f.write(URL + '\n')
        f.close()


        # INPUT: Name of file with URLs. Finds URLs that point to toronto.ca.


# OUTPUT: URLs to toronto.ca web pages, and toronto.ca IDs.
def get_toronto_urls(hyperlinks_text_file_name):
    f = open(hyperlinks_text_file_name, 'r')
    toronto_URLs = [];  # corresponding list of URLs to toronto.ca data
    toronto_IDs = [];  # corresponding IDs
    for line in f:
        #         print(line,end='')
        match = re.search('toronto.ca', line)  # match = re.search(pat, text)
        if match:
            ID = re.findall('toid=(.{14})', line)
            #             print(ID)
            URL = re.findall('(.+)&', line)
            #             print(URL)
            toronto_URLs.append(URL[0])
            toronto_IDs.append(ID[0])
    f.close()
    return (toronto_URLs, toronto_IDs)


# Convert title to file system name
def urlify_title(s):
    #     s = re.findall('(.+) - Data catalogue - Open Data', s)[0]
    s = re.findall('(.+) - (.+) - (.+)$', s)[0][0]
    s = re.sub(r'[^\w\s]', '', s)  # Remove all non-word characters (everything except numbers and letters)
    s = re.sub(r"\s+", '_', s)  # Replace all runs of whitespace with a single dash
    return s

# Get URLs to toronto.ca web pages, and toronto.ca IDs from the text file containing hyperlinks.
(toronto_URLs, toronto_IDs) = get_toronto_urls('sources/toronto.ca.txt')

# Main loop for downloading data from toronto.ca

for idx, toronto_URL in enumerate(toronto_URLs):
    if idx < 5: continue
    request = urllib.request.urlopen(toronto_URL)
    html = request.read()
    soup = BeautifulSoup(html, "lxml")

    title = urlify_title(soup.title.string);

    print("\n", idx, ": Processing data source:", title)

    # create metadata from original html
    metadata = create_metadata(html)

    folder_path = os.path.join(my_base_path, title + '_' + toronto_IDs[idx])
    print(folder_path)

    # create folder to download files to
    if not os.path.exists(folder_path):  os.makedirs(folder_path)

    # download original html
    orig_html_filename = toronto_IDs[idx] + '.html';
    download_file(toronto_URLs[idx], folder_path, orig_html_filename)

    # download all data resources
    clue_found = 0;
    bad_count = 0;

    for link in soup.find_all('a'):
        href = str(link.get('href'))

        if clue_found:  # output all URLs and exit the loop at the next URL containing "/wps/portal"
            if href.find('/wps/portal') >= 0:
                if bad_count > 10:
                    break
                bad_count += 1
            else:
                if href.find('City', 0, 5) >= 0:  # prepend 'http://www1.toronto.ca' to incomplete URLs starting with /City...
                    href = 'http://www1.toronto.ca' + urllib.parse.quote(href)
                print("  Downloading:", href)
                download_file(href, folder_path, rewrite=0)
                metadata['source_files'].append(href)
        elif href.find(
                'mailto:') >= 0:  # when found "mailto" clue, take all URLs until the URL that starts with "/wps/portal"
            clue_found = 1;

    # write metadata to disk
    fp = open(os.path.join(folder_path, 'metadata.json'), 'w');

    json.dump(metadata, fp)
    fp.close();

