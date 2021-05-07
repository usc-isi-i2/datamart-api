from requests import get
import json

# get all
# response = get('http://localhost:5000/metadata/datasets')
response = get('http://dsbox02.isi.edu:14080/metadata/datasets')
j = response.json()
print(json.dumps(j, indent=2))
print(response.status_code)

# get one
# response = get('http://localhost:5000/metadata/datasets/OECD')
# print(response.json())
