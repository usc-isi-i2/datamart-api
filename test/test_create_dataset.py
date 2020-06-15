from requests import post
wdi = {
    "name": "World Development Indicators",
    "dataset_id": "WDI2",
    "description": "Indicators from World Bank",
    "url": "https://data.worldbank.org/indicator"
}
gdp_ppp = {
    "name": "gross domestic product based on purchasing power parity",
    "variable_id": "GDP2"
}
wdi_response = post('http://127.0.0.1:5000/metadata/datasets?test&tsv', json=wdi)
print(wdi_response.text)
print(wdi_response.status_code)
wdi_response = post('http://127.0.0.1:5000/metadata/datasets', json=wdi)
print(wdi_response.text)
print(wdi_response.status_code)
gdp_response = post('http://127.0.0.1:5000/metadata/datasets/WDI2/variables?test&tsv', json=gdp_ppp)
print(gdp_response.text)
print(wdi_response.status_code)
gdp_response = post('http://127.0.0.1:5000/metadata/datasets/WDI2/variables', json=gdp_ppp)
print(gdp_response.text)
print((gdp_response.status_code))
