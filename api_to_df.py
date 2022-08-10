from closeio_api import Client
import pandas as pd
token_id = 'your_token_id'
api = Client(token_id)

resp = api.get('/user/')

df = pd.json_normalize(resp['data'])
df.head()
