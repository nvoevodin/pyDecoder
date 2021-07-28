import requests, json;
import pandas as pd;
import io;
import mysql.connector;
from sodapy import Socrata;
from datetime import date, timedelta;
import gc;
import string;
from fuzzywuzzy import fuzz;
from fuzzywuzzy import process;
import zipfile;
import requests, zipfile, io;
from sqlalchemy import create_engine;
from pushbullet import Pushbullet;


pb = Pushbullet('')




######################################################################################
def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=1):
    """
    df_1 is the left table to join
    df_2 is the right table to join
    key1 is the key column of the left table
    key2 is the key column of the right table
    threshold is how close the matches should be to return a match, based on Levenshtein distance
    limit is the amount of matches that will get returned, these are sorted high to low
    """
    s = df_2[key2].tolist()

    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit)) 
      
    df_1['matches'] = m
    df_1['scores'] = m

    m2 = df_1['matches'].apply(lambda x: ' '.join([i[0] for i in x if i[1] >= threshold]))
    m3 = df_1['scores'].apply(lambda x: ' '.join([ str(i[1]) for i in x if i[1] >= threshold]))
    
    df_1['matches'] = m2
    df_1['scores'] = m3

    return df_1
#################################################################################################




mydb = mysql.connector.connect(
  host="",
  user="root",
  passwd="",
  database = 'plagueDB'
)

mycursor = mydb.cursor()



fhv = pd.read_csv('https://data.cityofnewyork.us/api/views/8wbx-tsch/rows.csv?accessType=DOWNLOAD')['Vehicle VIN Number']

print('fhv success')

client = Socrata("data.cityofnewyork.us",
                 "",
                 username="",
                 password="")


today = date.today()


results = client.get("rhe8-mgbb", limit = 20000, last_updated_date=today)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results) 

if len(results_df) > 0:

  med = results_df['vehicle_vin_number']

  med = med.rename('Vehicle VIN Number')

  print('med success')


  vinAll = fhv.append(med)
  vinAll = pd.DataFrame(vinAll.reset_index())

  gc.collect()

  dbVins = pd.read_sql('select vin from decodedPlus',mydb)



  #2 find new ones and bind them to pulled vins

  newVins = vinAll[~vinAll['Vehicle VIN Number'].isin(dbVins['vin'])]

  newVins = newVins.rename(columns={'Vehicle VIN Number':'vin'})

  dbVinsWithNew = dbVins.append(newVins)

  #3 subtract vin from pulled vins + new vins to find the ones that are inactive now

  inactiveVins = dbVinsWithNew[~dbVinsWithNew['vin'].isin(vinAll['Vehicle VIN Number'])]

  par = tuple(list(inactiveVins['vin']))
  len(par)

  #4 delete inactive from the database

  countBefore = pd.read_sql("SELECT COUNT(*) FROM decodedPlus",mydb)

  if len(par) > 0:
    mycursor.execute('DELETE FROM decodedPlus WHERE vin IN {}'.format(par))


    mydb.commit()

    print(mycursor.rowcount, "record(s) deleted")

  countAfter = pd.read_sql("SELECT COUNT(*) FROM decodedPlus",mydb)

  print(countBefore-countAfter)





  #test = pd.read_csv('/home/nkta/Desktop/old/r_stuff/decoder/sample.csv')
  test = newVins['vin'].to_list()


    
  # Yield successive n-sized 
  # chunks from l. 
  def divide_chunks(l, n): 
        
      # looping till length l 
      for i in range(0, len(l), n):  
          yield l[i:i + n] 
    
  # How many elements each 
  # list should have 
  n = 49
    
  vins = list(divide_chunks(test, n))


  df = pd.DataFrame()

  mydb.close()

  if len(newVins) > 0:

    for i in vins:



        my_string = ';'.join(i)

        url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/';
        post_fields = {'format': 'json', 'data':f'{my_string}'};
        r = requests.post(url, data=post_fields);
        x = r.json()
        data = pd.DataFrame(x['Results'])[['VIN','Make','Model','ModelYear','FuelTypePrimary','FuelTypeSecondary']]
        df = df.append(data)
        print('done')	


    df['FuelTypePrimary'] = df['FuelTypePrimary'].str.strip()
    df['FuelTypeSecondary'] = df['FuelTypeSecondary'].str.strip()

    df.loc[df['FuelTypePrimary'] == '', 'FuelTypePrimary'] = None
    df.loc[df['FuelTypeSecondary'] == '' , 'FuelTypeSecondary'] = None




    df.loc[pd.isnull(df['FuelTypePrimary']) & pd.isnull(df['FuelTypeSecondary']), 'Type'] = 'Unknown'
    df.loc[(df['FuelTypePrimary'] == 'Gasoline') & (pd.isnull(df['FuelTypeSecondary'])), 'Type'] = 'Gasoline'
    df.loc[(df['FuelTypePrimary'] == 'Gasoline') & (df['FuelTypeSecondary'] == 'Electric'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Gasoline') & (df['FuelTypeSecondary'] == 'Ethanol (E85)'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Flexible Fuel Vehicle (FFV)') & (df['FuelTypeSecondary'] == 'Gasoline'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Flexible Fuel Vehicle (FFV)') & (pd.isnull(df['FuelTypeSecondary'])), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Electric') & (df['FuelTypeSecondary'] == 'Gasoline'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Diesel') & (pd.isnull(df['FuelTypeSecondary'])), 'Type'] = 'Diesel'
    df.loc[(df['FuelTypePrimary'] == 'Gasoline') & (df['FuelTypeSecondary'] == 'Flexible Fuel Vehicle (FFV)'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Flexible Fuel Vehicle (FFV), Gasoline') & (df['FuelTypeSecondary'] == 'Ethanol (E85)'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Gasoline, Flexible Fuel Vehicle (FFV)') & (df['FuelTypeSecondary'] == 'thanol (E85)'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Electric') & (pd.isnull(df['FuelTypeSecondary'])), 'Type'] = 'Electric'
    df.loc[(df['FuelTypePrimary'] == 'Electric, Gasoline') & (df['FuelTypeSecondary'] == 'Electric, Gasoline'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Gasoline') & (df['FuelTypeSecondary'] == 'Compressed Natural Gas (CNG)'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Electric, Gasoline') & (df['FuelTypeSecondary'] == 'Gasoline, Electric'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Gasoline, Flexible Fuel Vehicle (FFV)') & (df['FuelTypeSecondary'] == 'Gasoline'), 'Type'] = 'Hybrid'
    df.loc[(pd.isnull(df['FuelTypePrimary'])) & (df['FuelTypeSecondary'] == 'Gasoline'), 'Type'] = 'Gasoline'
    df.loc[(df['FuelTypePrimary'] == 'Compressed Natural Gas (CNG)') & (pd.isnull(df['FuelTypeSecondary'])), 'Type'] = 'CNG'
    df.loc[(df['FuelTypePrimary'] == 'Ethanol (E85)') & (pd.isnull(df['FuelTypeSecondary'])), 'Type'] = 'Ethanol'
    df.loc[(df['FuelTypePrimary'] == 'Flexible Fuel Vehicle (FFV)') & (df['FuelTypeSecondary'] == 'Electric'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Flexible Fuel Vehicle (FFV), Gasoline') & (df['FuelTypeSecondary'] == 'Gasoline'), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Electric, Gasoline') & (pd.isnull(df['FuelTypeSecondary'])), 'Type'] = 'Hybrid'
    df.loc[(df['FuelTypePrimary'] == 'Liquefied Petroleum Gas (propane or LPG)') & (pd.isnull(df['FuelTypeSecondary'])), 'Type'] = 'LPG'

    df['decoded'] = date.today()



    df.columns=['vin','make','model','year','FuelTypePrimary','FuelTypeSecondary','Type', 'Decoded']
    df.year = df.year.astype(int)

    ########################################################################



    years = pd.DataFrame(df['year'].unique())
    years = years.dropna()

    r = requests.get('https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip')
    z = zipfile.ZipFile(io.BytesIO(r.content))
    dataMain = pd.read_csv(z.open('vehicles.csv'))



    dataMain = dataMain.loc[:,['comb08','barrels08','fuelType','fuelType1','highway08', 'make', 'model','year']]

    finalData = pd.DataFrame(columns = ['vin','id','matches','scores','comb08'])

    for y in years[0]:
      

      data = dataMain[dataMain.year == int(y)]



    #fuels = list(data['fuelType'].unique())

      data.loc[data['fuelType'].isin(['Regular','Premium','Midgrade']) , 'fuelType'] = 'Gasoline'
      data.loc[data['fuelType'].isin(['Electricity']) , 'fuelType'] = 'Electric'
      data.loc[data['fuelType'].isin(['Diesel']) , 'fuelType'] = 'Diesel'
      data.loc[data['fuelType'].isin(['Premium or E85','Gasoline or natural gas','Gasoline or propane','Regular Gas or Electricity','Premium and Electricity','Gasoline or E85','Regular Gas and Electricity','Premium Gas or Electricity']) , 'fuelType'] = 'Hybrid'

      data["make"] = data["make"].str.replace('[^\w\s]',' ')
      #data["make"] = data["make"].str.split(' ', 1).str[0]

      #data["model"] = data["model"].str.replace('[^\w\s]',' ')
      data["model"] = data["model"].str.split(' ', 1).str[0]

      data['id'] = data['make'] + ' '+ data['model'] +' '+ data['fuelType']
      data['id'] = data['id'].str.lower()

      grouped = data.groupby(['id'], as_index=False)['comb08'].mean()

      #grouped.loc[grouped['comb08']==grouped['comb08'].max()]



      # mycursor = mydb.cursor()

      # mycursor.execute('select * from decodedVINS where year=%s', (int(y),))
      # vehicles = pd.DataFrame(mycursor.fetchall())
      vehicles = df[df['year'] == int(y)]

      fuels1 = list(vehicles['Type'].unique())

      vehicles["make"] = vehicles["make"].str.replace('[^\w\s]',' ')
      #vehicles["make"] = vehicles["make"].str.split(' ', 1).str[0]

      #vehicles["model"] = vehicles["model"].str.replace('[^\w\s]',' ')
      vehicles["model"] = vehicles["model"].str.split(' ', 1).str[0]

      vehicles['id'] = vehicles['make'] + ' '+ vehicles['model'] +' '+ vehicles['Type']
      vehicles['id'] = vehicles['id'].str.lower()


      test = vehicles[['vin','id']]








      fuzzy = fuzzy_merge(test, grouped, 'id', 'id', threshold=92)
      grouped = grouped.rename(columns={'id':'matches'})

      fuzzy = fuzzy.merge(grouped, on='matches', how='left')

      finalData = finalData.append(fuzzy)
      print(y)

    finalData = finalData.iloc[:,[0,4]]

    finalData = finalData.rename(columns={'comb08':'mpg'})



    df = df.merge(finalData, on='vin', how='left')


    engine = create_engine("mysql+pymysql://{user}:{pw}@/{db}"
                          .format(user="root",
                                  pw="",
                                  db="plagueDB"))




    df.to_sql('decodedPlus', engine, if_exists='append', chunksize=1000)
    engine.dispose()

    print(f'inserted {len(newVins)}, removed {len(inactiveVins)}')
    push = pb.push_note(f'inserted {len(newVins)}, removed {len(inactiveVins)}', "This")
  else:
    print('no new vins')
    push = pb.push_note('no new vins', "This")

else:
  push = pb.push_note('no meds', "")






