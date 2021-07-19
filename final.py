import pandas as pd
import sqlite3 as sqlite

df = pd.DataFrame()


def readsourcefile():
    try:
        df = pd.read_csv("source.txt", sep='|')
        df = df.iloc[:, 2:]
        df['DOB'] = pd.to_datetime(df['DOB'], format='%d%m%Y')
        df['Open_Date'] = pd.to_datetime(df['Open_Date'], format='%Y%m%d')
        df['Last_Consulted_Date'] = pd.to_datetime(
            df['Last_Consulted_Date'], format='%Y%m%d')
        return df
    except Exception:
        print("Unable to read the source file / There may be any missing fields or invalid entries")


def stagingdb():
    try:
        con = sqlite.connect("database.db")
        df.to_sql(name='staging_table', con=con)
    except Exception:
        print("Error in connecting to DB")


def loading():
    try:
        con = sqlite.connect("database.db")
        df = pd.read_sql_query("SELECT * from staging_table", con)
        df = df.iloc[:, 1:]
        for i in df.index:
            country = df['Country'][i]
            with con:
                cur = con.cursor()
                str1 = """ CREATE TABLE IF NOT EXISTS """+"""Table_"""+country+"""(
                                                customer_name VARCHAR(255) NOT NULL PRIMARY KEY,
                                                customer_id VARCHAR(255) NOT NULL,
                                                customer_open_date DATE(8) NOT NULL,
                                                last_consulted_date DATE(8),
                                                vaccination_type TEXT(5),
                                                doctor_consulted TEXT(255),
                                                state TEXT(5),
                                                country TEXT(5),
                                                dob DATE(8),
                                                active_customer TEXT(1)
                                            ) """
                cur.execute(str1)

                inputs = [df['Customer_Name'][i], str(df['Customer_Id'][i]), str(df['Open_Date'][i]), str(df['Last_Consulted_Date'][i]), df['Vaccination_Id'][i],
                          df['Dr_Name'][i], df['State'][i], df['Country'][i], str(df['DOB'][i]), df['Is_Active'][i]]

                str2 = """INSERT INTO """+"""Table_"""+country+"""(
                                                customer_name,customer_id,customer_open_date,last_consulted_date,vaccination_type,doctor_consulted,state,country,dob,active_customer) VALUES (?,?,?,?,?,?,?,?,?,?)"""
                cur.execute(str2, inputs)
    except Exception:
        print("Error in loading")


df = readsourcefile()
stagingdb()
loading()
