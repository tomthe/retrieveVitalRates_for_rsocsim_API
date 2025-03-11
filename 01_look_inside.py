#%%
import duckdb
import pandas as pd
import buckaroo
#%%
fn1 = "inputdata/WPP2024_DeathsBySingleAgeSex_Medium_1950-2023.csv.gz"
fn2 = "inputdata/WPP2024_DeathsBySingleAgeSex_Medium_2024-2100.csv.gz"
fn3 = "inputdata/WPP2024_Fertility_by_Age1.csv.gz"
fn4 = "inputdata/WPP2024_Life_Table_Abridged_Medium_1950-2023.csv.gz"
fn5 = "inputdata/WPP2024_Life_Table_Complete_Medium_Female_1950-2023.csv.gz"
#%%
con = duckdb.connect(database=':memory:', read_only=False)
con.execute(f"CREATE TABLE deaths AS SELECT * FROM read_csv_auto('{fn1}')")
#%%
con.execute(f"CREATE TABLE deaths2 AS SELECT * FROM read_csv_auto('{fn2}')")
con.execute(f"CREATE TABLE fertility AS SELECT * FROM read_csv_auto('{fn3}')")
# %%
q = "SELECT * FROM deaths LIMIT 5"
con.execute(q).fetchdf()
# %%
q = "SELECT * FROM deaths2 LIMIT 5"
con.execute(q).fetchdf()    

# %%
q = "SELECT * FROM fertility --LIMIT 5"
df = con.execute(q).fetchdf()
df.sample(200)
# %%

# %%
q = "SELECT * FROM deaths --LIMIT 5"
df = con.execute(q).fetchdf()
df.sample(200)
# %%
df4 = con.execute(f"SELECT * FROM read_csv_auto('{fn4}')").fetchdf()
df4.sample(200)

# %%
df5 = con.execute(f"SELECT * FROM read_csv_auto('{fn5}')").fetchdf()
df5.sample(1000)
# %%
