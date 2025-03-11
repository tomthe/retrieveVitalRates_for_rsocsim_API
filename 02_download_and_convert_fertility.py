#%%
import pandas as pd
import duckdb
import buckaroo
import os
import requests
#%%
# Download the data
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
    zero_rows = []
    for year in years:
        zero_rows.append({'year': year, 'AgeGrpStart': 0, 'Age_up': 12, 'Month': 0, 'ASFR_mo': 0.0})
        zero_rows.append({'year': year, 'AgeGrpStart': 56, 'Age_up': 111, 'Month': 0, 'ASFR_mo': 0.0})
    return pd.concat([df, pd.DataFrame(zero_rows)], ignore_index=True)
#%%
# create rate files for each country and year
for iso2 in dfiso2["ISO2_code"][1:2]:
    q = f"""SELECT Time as year, AgeGrpStart, ASFR FROM fertility 
    WHERE ISO2_code = '{iso2}'
      AND AgeGrpSpan = 1
      AND Variant = 'Medium'
    ORDER BY Time, AgeGrpStart ASC"""
    dfc = con.execute(q).fetchdf()
    # Transform the data
    dfc['AgeGrpStart'] = dfc['AgeGrpStart'].replace({'12-': 12, '55+': 55}).astype(int)
    dfc['Age_up'] = dfc['AgeGrpStart'] + 1
    dfc['Month'] = 0
    dfc['ASFR_mo'] = dfc['ASFR'] / 12
    dfc = dfc.drop(columns=['ASFR'])
    
    # Add rows with rates = 0 for ages 0-12 and 56-111
    dfc = dfc.groupby('year').apply(add_zero_rows).reset_index(drop=True)


    for year in dfyears["Time"]:
        dfcy = dfc[dfc["year"] == year]
        fn = f"outputdata/{iso2}/socsim_fert_{iso2}_{year}.txt"
        # create the directory if it does not exist
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        with open(fn, "w") as f:
            f.write(f"** Period (Monthly) Age-Specific Fertility Rates for {iso2} in {year}\n")
            f.write("* Retrieved from the Human Fertility Database, www.humanfertility.org\n")
            f.write("* Max Planck Institute for Demographic Research (Germany) and\n")
            f.write("* Vienna Institute of Demography (Austria)\n")
            f.write(f"* Data downloaded on {pd.Timestamp.now().strftime('%d %b %Y %X %Z')}\n")
            f.write("** NB: The original HFD annual rates have been converted into monthly rates\n")
            f.write("** The open age interval (55+) is limited to one year [55-56)\n\n")

            # Print birth rates (single females)
            f.write("birth 1 F single 0\n")
            for _, row in dfcy.iterrows():
                f.write(f"{row['Age_up']} 0 {row['ASFR_mo']:.5f}\n")
            f.write("\n")

            # Print birth rates (married females)
            f.write("birth 1 F married 0\n")
            for _, row in dfcy.iterrows():
                f.write(f"{row['Age_up']} 0 {row['ASFR_mo']:.5f}\n")



# - * comments with *
# - definition of block: """birth 1 F married 0""" - event, group??, gender, married, parity
# - definition of one range: """"56 0 0.00083""" - age until, group???, monthly rate?
# - 2 blocks: 1 for single. one for married. same rates
# %%
