import requests
import json

def find_root(block_ids, data):
    for id in block_ids:
        if data['blocks'][id]['block_id'] == "course":
            return id

def print_child(block_id, data, tab_size):
    print("   " * tab_size, data[block_id]['display_name'])
    if data[block_id].get('children'):
        for child in data[block_id].get('children'):
            print_child(child, data, tab_size + 1)

url = "http://analytics.skillfactory.ru:5000/api/v1.0/get_structure_course/"
answer = requests.post(url)
data = json.loads(answer.text)

with open("answer.txt", 'a') as outfile:
    outfile.write(answer.text)
outfile.close()
#print(data.keys())
#print(data['root'])
block_ids = data['blocks'].keys()
root = find_root(block_ids, data)
print_child(root, data['blocks'], 0)
