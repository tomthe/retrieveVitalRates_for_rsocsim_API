#%%
import pandas as pd
import duckdb
import buckaroo
import os
import requests
import zipfile
#%%
# Download the data
source = "UN"
#%%
url = "https://population.un.org/wpp/assets/Excel Files/1_Indicator (Standard)/CSV_FILES/WPP2024_Fertility_by_Age1.csv.gz"
fn = "download/WPP2024_Fertility_by_Age1.csv.gz"
if not os.path.exists(fn):
    r = requests.get(url)
    with open(fn, 'wb') as f:
        f.write(r.content)
#%%
# Load the data
con = duckdb.connect(database=':memory:', read_only=False)
con.execute(f"CREATE TABLE fertility AS SELECT * FROM read_csv_auto('{fn}')")
#%%
# Look inside the data
q = "SELECT * FROM fertility LIMIT 5"
con.execute(q).fetchdf()

#%%
# create a list of unique countries
q = "SELECT DISTINCT ISO2_code FROM fertility"
dfiso2 = con.execute(q).fetchdf()
dfiso2.head(10)
#%%
# create a list of unique years
q = "SELECT DISTINCT Time FROM fertility, order by Time ASC"
dfyears = con.execute(q).fetchdf()
#%%

# Add rows with rates = 0 for ages 0-12 and 56-111
def add_zero_rows(df):
    years = df['year'].unique()
    youngest_age = df['AgeGrpStart'].min()
    oldest_age = df['AgeGrpStart'].max()
    zero_rows_young = []
    zero_rows_old = []
    for year in years:
        zero_rows_young.append({'year': year, 'AgeGrpStart': 0, 'Age_up': youngest_age, 'Month': 0, 'ASFR_mo': 0.0})
        zero_rows_old.append({'year': year, 'AgeGrpStart': oldest_age+1, 'Age_up': 111, 'Month': 0, 'ASFR_mo': 0.0})
    return pd.concat([pd.DataFrame(zero_rows_young),df, pd.DataFrame(zero_rows_old)], ignore_index=True)
#%%
# create rate files for each country and year
for iso2 in dfiso2["ISO2_code"]:
    q = f"""SELECT Time as year, AgeGrpStart, ASFR 
    FROM fertility 
    WHERE ISO2_code = '{iso2}'
      AND AgeGrpSpan = 1
      AND Variant = 'Medium'
    ORDER BY Time, AgeGrpStart ASC"""
    dfc = con.execute(q).fetchdf()
    # Transform the data
    dfc['AgeGrpStart'] = dfc['AgeGrpStart'].replace({'12-': 12, '55+': 55}).astype(int)
    dfc['Age_up'] = dfc['AgeGrpStart'] + 1
    dfc['Month'] = 0
    dfc['ASFR_mo'] = dfc['ASFR'] / 12.0 / 1000
    # dfc = dfc.drop(columns=['ASFR'])
    
    # Add rows with rates = 0 for ages 0-12 and 56-111
    dfc = dfc.groupby('year').apply(add_zero_rows).reset_index(drop=True)

    directory = f"outputdata/{source}/{iso2}/"
    os.makedirs(os.path.dirname(directory), exist_ok=True)
    zip_filename = f"{directory}socsim_fert_{iso2}_rates.zip"
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as country_zip:
        for year in dfyears["Time"]:
            dfcy = dfc[dfc["year"] == year]
            fn_in_zip = f"socsim_mort_{iso2}_{year}.txt"
            
            with country_zip.open(fn_in_zip, 'w') as zipf:
                content = f"""** Period (Monthly) Age-Specific Fertility Rates for {iso2} in {year}
* Data downloaded on {pd.Timestamp.now().strftime('%d %b %Y %X %Z')}
* Source: {source}, downloaded from {url}
** NB: The original annual rates have been converted into monthly rates by dividing by 12 (and 1000)
** The open age interval (55+) is limited to one year [55-56)\n"""


            # Print birth rates (single females)
                content +="birth 1 F single 0\n"
                for _, row in dfcy.iterrows():
                    content +=f"{row['Age_up']} 0 {row['ASFR_mo']:.5f}\n"
                content += "\n"

                # Print birth rates (married females)
                content +="birth 1 F married 0\n"
                for _, row in dfcy.iterrows():
                    content += f"{row['Age_up']} 0 {row['ASFR_mo']:.5f}\n"
                content += "\n"
                zipf.write(content.encode())

    print(f"Created {zip_filename}")

# - * comments with *
# - definition of block: """birth 1 F married 0""" - event, group??, gender, married, parity
# - definition of one range: """"56 0 0.00083""" - age until, group???, monthly rate?
# - 2 blocks: 1 for single. one for married. same rates
# %%
