from requests import get
import json

# get all
response = get('http://localhost:5000/metadata/datasets')
j = response.json()
print(json.dumps(j))
print(response.status_code)

# get one
response = get('http://localhost:5000/metadata/datasets/OECD')
print(response.json())
