import requests
import json
import os
import fnmatch

URL = 'http://retrotector.neuro.uu.se/pub/ws/http/'
indexer = 0
processing_files = []

def one_post(given_link):
    with open(given_link, 'rb') as f:
        files = {'file': f}
        n = requests.post(f'{URL}enqueue?format=json', files= files)
        lib = json.loads(n.text)
        return lib[0]['jobid']

def all_finished():
    response = requests.get(f'{URL}queue?format=json')
    lib = json.loads(response.text)
    status = [el['state'] for el in lib]
    return True if status.count('finished') == len(status) else False

def kill_all():
    file = requests.get(f'{URL}queue?format=json')
    lib = json.loads(file.text)
    statuses = [[el['jobid'], str(el['result'])] for el in lib]
    for el in statuses:
        requests.post(f'{URL}dequeue?jobid={el[0]}&result={el[1]}')

def get_current_credentials():
    file = requests.get(f'{URL}queue?format=json')
    lib = json.loads(file.text)
    statuses = [[el['jobid'], str(el['result'])] for el in lib]
    return statuses

def one_download(job_id, result_id):
    file = requests.get(f'{URL}readdir?jobid={job_id}&result={result_id}&format=json')
    result_files = []
    os.mkdir(f'result{job_id}')
    lib = json.loads(file.text)
    for i in lib:
        if i.startswith('result'):
            result_files.append(i)
    for elem in result_files:
        processed_file = requests.get(f'{URL}fopen?jobid={job_id}&result={result_id}&file={elem}',stream = True)
        #print(f'{URL}fopen?jobid={job_id}&result={result_id}&file={elem}')
        with open("result{}/{}".format(job_id, elem.replace('/','|')),'wb') as f:
            f.write(processed_file.content)


infiles = [f for f in os.listdir('.') if os.path.isfile(f)]
for f in infiles:
    if (os.stat(f).st_size) < 10000000 and (fnmatch.fnmatch(f, '*.fna*') or fnmatch.fnmatch(f, '*.fa*') or fnmatch.fnmatch(f, '*.fasta*')):
        processing_files.append(str(f))

parts_processing = [processing_files[x:x+10] for x in range(0, len(processing_files), 10)]


while indexer < len(parts_processing):
    curr_try = parts_processing[indexer]
    ids = []
    for elem in curr_try:
        ids.append(str(one_post(elem)))
    while all_finished = False:
        time.sleep(30)
    check = get_current_credentials()
    for elem in check:
        one_download(elem[0], elem[1])
    kill_all()
