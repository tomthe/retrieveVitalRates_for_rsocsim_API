#%%
import duckdb
import requests
import gzip
import shutil
import pandas as pd
import zipfile

tablename = "mall"
source = "UN"

# URL of the file to download
malefemales = ["Male","Female"]
timespans = ["1950-2023", "2024-2100"]
#%%
for malefemale in malefemales:
    for timespan in timespans:
        url = f"https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_{malefemale}_{timespan}.csv.gz"

        # Local filename to save the downloaded file
        fn = f"inputdata/WPP2024_Life_Table_Complete_Medium_{malefemale}_{timespan}.csv.gz"

        # # Download the file
        if not os.path.exists(fn):
            r = requests.get(url)
            with open(fn, 'wb') as f:
                f.write(r.content)

# Load the CSV into DuckDB
con = duckdb.connect(database=':memory:')
con.execute(f"""
    CREATE TABLE mall AS
    SELECT * FROM './inputdata/WPP2024_Life_Table_Complete_Medium*.csv.gz'
""").df()

# %%
con.execute(f"""Create table m2 as select * from read_csv_auto('{fn2}')""").df()

# %%
con.execute(f"""SELECT * FROM  mall LIMIT 300""").df()
# %%
import buckaroo

# %%
df2 = con.execute(f"""SELECT * FROM mall""").df()
df2.sample(100)
# %%
####################################################################################
#%%

# Create a list of unique countries
q = "SELECT DISTINCT ISO2_code FROM mall"
dfiso2 = con.execute(q).fetchdf()

# Create a list of unique years
q = "SELECT DISTINCT Time FROM mall ORDER BY Time ASC"
dfyears = con.execute(q).fetchdf()


# Add rows with rates = 0 for ages 0-12 and 56-111
def add_death_rows(df):
    years = df['year'].unique()
    oldest_age = df['AgeGrpStart'].max()
    death_rows_old = []
    for year in years:
        death_rows_old.append({'year': year, 'Sex':'Male', 'AgeGrpStart': oldest_age + 1, 'Age_up': oldest_age + 2, 'Month': 0, 'qx':0.9, 'qx_monthly': 0.9999999999})
        death_rows_old.append({'year': year, 'Sex':'Female', 'AgeGrpStart': oldest_age + 1, 'Age_up': oldest_age + 2, 'Month': 0, 'qx':0.9, 'qx_monthly': 0.9999999999})
    return pd.concat([df, pd.DataFrame(death_rows_old)], ignore_index=True)

#%%
##########

# Create rate files for each country and year
for iso2 in dfiso2["ISO2_code"]:
    q = f"""SELECT Time as year, AgeGrpStart, qx, 1-(1-qx)**(1/12) as qx_monthly,
    Sex, ISO2_code, Variant
    FROM {tablename} 
    WHERE ISO2_code = '{iso2}'
      AND AgeGrpSpan = 1
      AND Variant = 'Medium'
    ORDER BY Time, AgeGrpStart ASC"""
    dfc = con.execute(q).fetchdf()
    
    # Transform the data
    dfc['AgeGrpStart'] = dfc['AgeGrpStart'].replace({'12-': 12, '55+': 55}).astype(int)
    dfc['Age_up'] = dfc['AgeGrpStart'] + 1
    dfc['Month'] = 0
    
    oldest_age = dfc['Age_up'].max()
    # Add rows with rates = 0 for ages 0-12 and 56-111
    # dfc = dfc.groupby('year').apply(add_death_rows).reset_index(drop=True)
    
    # Create the directory if it does not exist
    directory = f"outputdata/{source}/{iso2}/"
    os.makedirs(os.path.dirname(directory), exist_ok=True)
    zip_filename = f"{directory}socsim_mort_{iso2}_rates.zip"
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as country_zip:
        for year in dfyears["Time"]:
            dfcy = dfc[dfc["year"] == year]
            fn_in_zip = f"socsim_mort_{iso2}_{year}.txt"
            
            with country_zip.open(fn_in_zip, 'w') as zipf:
                content = f"**qx Period (Monthly) Age-Specific Mortality Rates for {iso2} in {year}\n"
                content += f"* Data downloaded on {pd.Timestamp.now().strftime('%d %b %Y %X %Z')}\n"
                content += f"* Source: {source}, downloaded from {url} and 3 others\n"
                content += f"** NB: The original annual rates have been converted into monthly rates by 1-(1-qx_M)**(1/12) \n"
                content += "** The open age interval (100+) is limited to one year\n\n"

                dfcyfemale = dfcy[dfcy["Sex"] == "Female"]
                # Print birth rates (single females)
                content += "death 1 F single 0\n"
                for _, row in dfcyfemale.iterrows():
                    content += f"{row['Age_up']} 0 {row['qx_monthly']:.7f}\n"
                content += f"{oldest_age + 1} 0 {0.99999:.7f}\n"

                content += "\n"

                # Print death rates (single males)
                dfcymale = dfcy[dfcy["Sex"] == "Male"]
                content += "death 1 M single 0\n"
                for _, row in dfcymale.iterrows():
                    content += f"{row['Age_up']} 0 {row['qx_monthly']:.7f}\n"
                content += f"{oldest_age + 1} 0 {0.99999:.7f}\n"

                zipf.write(content.encode('utf-8'))
    print(f"Created {zip_filename}", f"{oldest_age=}")

# %%
