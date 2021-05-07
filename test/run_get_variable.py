import json
import requests

# get all
# response = requests.get('http://localhost:5000/metadata/datasets/OECD/variables')
# print(json.dumps(response.json()))
# print(len(response.json()))
# print(response.status_code)


# get one
# response = requests.get('http://localhost:5000/metadata/datasets/OECD/variables/gdp_per_capita')
# print(json.dumps(response.json()))
# print(response.status_code)
#
# response = requests.get('http://localhost:5000/datasets/OECD/variables/road_fatalities')
# # print(json.dumps(response.json()))
# print(response.text)
# print(response.status_code)

response = requests.get('http://localhost:5000/datasets/UAZ/variables/VUAZ-9')
print(response.text)