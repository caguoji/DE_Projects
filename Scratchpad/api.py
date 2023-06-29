import requests
import json
import pandas as pd

f = requests.get("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/rates_of_exchange?fields=country_currency_desc,exchange_rate, record_date&filter=record_date:gte:2015-01-01")

#print(f.content)

#j_file = json.load(f.content)
#print(j_file)

#print(f.text)
ftext=f.text
#pd.DataFrame(ftext)
#df = pd.read_json(ftext)
#df.head()

result = json.loads(ftext)
_lst = result['data']

df_out = pd.DataFrame(_lst)

df_out.to_csv('output/df_out.csv')
