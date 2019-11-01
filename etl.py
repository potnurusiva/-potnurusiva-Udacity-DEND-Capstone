import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_immigration_file(cur, fname, conn):
    """Process the given immigration data file in the file path, extract the required columns from it and finally load it into the tables """
    # Read immigration SAS file
    df = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")
    df.fillna(0,inplace=True)

    # using dictionary to convert specific columns 
    convert_dict = {'cicid': int, 
                    'i94yr': int,
                    'i94mon' : int,
                    'i94visa' : int,
                    'count' : int,
                    'biryear' : int
                   } 
    df = df.astype(convert_dict)
    
    # Fetching required columns from the data
    immigration_data = df.iloc[:,[0,1,2,5,8,11,12,20,22,27]].values.tolist()
    
    # for i in range(len(immigration_data)):
    for i in range(1000000):
        try:
            cur.execute(dimimmigration_table_insert, immigration_data[i])
        except psycopg2.IntegrityError as e:
            conn.rollback()
            print('insert failure:',e)
            #continue
        except ValueError as e:
            conn.rollback()
            print('insert failure:',e)
            #continue
        else:
            conn.commit()


def process_airport_file(cur, filepath, conn):
    """Process the given airport codes data file in the file path, extarct the required columns from it and finally load it into the tables """
    # Read airport codes data file
    df2 = pd.read_csv('airport-codes_csv.csv')
    df2.fillna(0,inplace=True)

    #Fetching required columns from the data
    airports_data = df2.iloc[:,[0,2,1,6,7,8,10]].values.tolist()
    
    # insert airport codes data records
    for i in range(len(airports_data)):
        try:
            cur.execute(dimairports_table_insert, airports_data[i])
        except psycopg2.IntegrityError as e:
            conn.rollback()
            print('insert failure:',e)
            #continue
        except ValueError as e:
            conn.rollback()
            print('insert failure:',e)
            #continue
        else:
            conn.commit()
            
def process_city_file(cur, filepath, conn):
    """Process the given cities data file in the file path, extarct the required columns from it and finally load it into the tables """
    # Read cityies information data file
    df3 = pd.read_csv('us-cities-demographics.csv',sep = ';')
    df3.fillna(0,inplace=True)
    
    # using dictionary to convert specific columns 
    convert_dict = {'Male Population': int, 
                    'Female Population': int
                   } 
    df3 = df3.astype(convert_dict)

    #Fetching required columns from the data
    cities_data = df3.iloc[:,[0,1,9,2,3,4,5]].values.tolist()
    
    # insert city data records
    for i in range(len(cities_data)):
        try:
            cur.execute(dimcities_table_insert, cities_data[i])
        except psycopg2.IntegrityError as e:
            conn.rollback()
            print('insert failure:',e)
            #continue
        except ValueError as e:
            conn.rollback()
            print('insert failure:',e)
            #continue
        else:
            conn.commit()



def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=flyingdb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, fname='../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat', func=process_immigration_file)
    process_data(cur, conn, filepath='airport-codes_csv.csv', func=process_airport_file)
    process_data(cur, conn, filepath='us-cities-demographics.csv', func=process_city_file)

    #Fetching information from dimension tables and loading into fact tables
    cur.execute(fact_select)
    results = cur.fetchall()
    
    for row in results:
        #print(row)
        if row:
            city, identity, travelid, passengercount = row
        else:
            city, identity, travelid, passengercount = None, None, None, None
        fact_table_data = [city, identity, travelid, passengercount]
    
        try:
            cur.execute(factflying_table_insert, fact_table_data)
            print(fact_table_data)
        except psycopg2.IntegrityError as e:
            conn.rollback()
            print('insert failure:',e)
            #continue
        except ValueError as e:
            conn.rollback()
            print('insert failure:',e)
            #continue
        else:
            conn.commit()

    conn.close()


if __name__ == "__main__":
    main()