#%% 
from trino.dbapi import connect
from trino.auth import BasicAuthentication

#%%
conn = connect(host='<prod trino host>',port=443,http_scheme='https',catalog='delta',auth=BasicAuthentication('sqlreadonly','<password>'),verify=True)
cur = conn.cursor(legacy_primitive_types=True)
cur.execute("select * from delta.analytics.applications limit 10")
rows = cur.fetchall()

for row in rows:
    print(row)

cur.close()
conn.close()

# %% 