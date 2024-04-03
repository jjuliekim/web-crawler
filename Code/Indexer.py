import json
import os
import re

crawled_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/crawled.json'
with open(crawled_file, 'r') as json_file:
    crawled_items = json.load(json_file)

inlinks_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/inlinks.json'
with open(inlinks_file, 'r') as json_file:
    inlinks = json.load(json_file)

outlinks_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/outlinks.json'
with open(outlinks_file, 'r') as json_file:
    outlinks = json.load(json_file)

wave_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/wave.json'
with open(wave_file, 'r') as json_file:
    wave = json.load(json_file)

index_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/index_1.json'
index = {}

parsed_file = 'C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Stored/content_1.json'


def parse_files(path):
    doc_no_pattern = re.compile(r"<DOCNO>(.+?)</DOCNO>")
    text_pattern = re.compile(r"<TEXT>(.*?)</TEXT>", re.DOTALL)
    with open(path, 'r', encoding='utf-8') as f:
        doc_no = ""
        content = ""
        in_text = False
        for line in f:
            if line.startswith("<DOCNO>"):
                match = doc_no_pattern.search(line)
                if match:
                    doc_no = match.group(1).strip()
            elif line.startswith("<TEXT>"):
                in_text = True
                text_match = text_pattern.search(line)
                if text_match:
                    content += text_match.group(1)
            elif in_text and "</TEXT>" not in line:
                cleaned_line = ''.join(c for c in line if c.isalnum() or c.isspace() or c == '-')
                content += cleaned_line
            elif in_text and "</TEXT>" in line:
                in_text = False
            elif line.startswith("</DOC>"):
                doc_content[doc_no] = content.strip()
                doc_no = ""
                content = ""
    with open(parsed_file, 'w', encoding='utf-8') as json_file:
        json.dump(doc_content, json_file, indent=2)


if os.path.exists(parsed_file):
    with open(parsed_file, 'r') as json_file:
        doc_content = json.load(json_file)
else:
    doc_content = {}
    # parse_files('C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Parsed_Docs/docs_0.txt')
    # print('doc 0')
    parse_files('C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Parsed_Docs/docs_1.txt')
    print('doc 1')
    # parse_files('C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Parsed_Docs/docs_2.txt')
    # print('doc 2')
    # parse_files('C:/Users/julie/PycharmProjects/hw3-jjuliekim-reclone/hw3-jjuliekim/Results/Parsed_Docs/docs_3.txt')
    # print('doc 3')

num = 1
print(len(doc_content))
for url in doc_content.keys():
    try:
        values = inlinks[url]
    except KeyError:
        inlinks[url] = []
    try:
        values = outlinks[url]
    except KeyError:
        outlinks[url] = []
    data = {
        "content": doc_content[url],
        "inlinks": inlinks[url],
        "outlinks": outlinks[url],
        "author": "Julie Kim"
    }
    index[url] = data
    print(num)
    num += 1
    if num % 100 == 0:
        with open(index_file, 'w') as json_file:
            json.dump(index, json_file, indent=2)

with open(index_file, 'w') as json_file:
    json.dump(index, json_file, indent=2)
