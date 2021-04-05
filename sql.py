from __future__ import (absolute_import, division, print_function)
from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import json
import os

import warnings
warnings.filterwarnings('ignore')
from matplotlib.axes._axes import _log as matplotlib_axes_logger
matplotlib_axes_logger.setLevel('ERROR')

data_path = "."

def pgconnect(credential_filepath, db_schema="public"):
    # please replace <your_unikey> and <your_SID> with your own details in the credentials file!
    with open(credential_filepath) as f:
        db_conn_dict = json.load(f)
        HOST       = db_conn_dict['host']
        DB_USER    = db_conn_dict['user']
        DB_PW      = db_conn_dict['password']
        DEFAULT_DB = db_conn_dict['user']

        try:
            db = create_engine('postgres+psycopg2://'+DB_USER+':'+DB_PW+'@'+HOST+'/'+DEFAULT_DB, echo=False)
            conn = db.connect()
            print('connected')
        except Exception as e:
            print("unable to connect to the database")
            print(e)
        return db,conn

credfilepath = os.path.join(data_path, "db.json")

# 1st: login to database
db,conn = pgconnect(credfilepath)

def pgexecute( conn, sqlcmd, args=None, msg='', silent=False ):
    """ utility function to execute some SQL query statement
       can take optional arguments to fill in (dictionary)
       will print out on screen the result set of the query
       error and transaction handling built-in """
    retval = False
    result_set = None

    try:
        if args is None:
            result_set = conn.execute(sqlcmd).fetchall()
        else:
            result_set = conn.execute(sqlcmd, args).fetchall()

        if silent == False:
            print("success: " + msg)
            for record in result_set:
                print(record)
        retval = True
    except Exception as e:
        if silent == False:
            print("db read error: ")
            print(e)
    return retval

def pgquery(conn, sqlcmd, args=None, silent=False ):
    """ utility function to execute some SQL query statement
    can take optional arguments to fill in (dictionary)
    will print out on screen the result set of the query
    error and transaction handling built-in """
    retdf = pd.DataFrame()
    retval = False
    try:
        if args is None:
            retdf = pd.read_sql_query(sqlcmd,conn)
        else:
            retdf = pd.read_sql_query(sqlcmd,conn,params=args)
        if silent == False:
            print(retdf.shape)
            print(retdf.to_string())
        retval = True
    except Exception as e:
        if silent == False:
            print("db read error: ")
            print(e)
    return retval,retdf

# load datasets
single_df = pd.read_csv('statcast_single.csv', low_memory=False)
double_df = pd.read_csv('statcast_double.csv', low_memory=False)
triple_df = pd.read_csv('statcast_triple.csv', low_memory=False)
hr_df = pd.read_csv('statcast_hr.csv', low_memory=False)
fieldOut_df = pd.read_csv('statcast_fieldOut.csv', low_memory=False)

table_name1 = "singles"
single_df.to_sql(table_name1, con=conn, if_exists='replace',index=False)

table_name2 = "doubles"
double_df.to_sql(table_name2, con=conn, if_exists='replace',index=False)

table_name3 = "triples"
triple_df.to_sql(table_name3, con=conn, if_exists='replace', index=False)

table_name4 = "home_runs"
hr_df.to_sql(table_name4, con=conn, if_exists='replace',index=False)

table_name5 = "field_outs"
fieldOut_df.to_sql(table_name4, con=conn, if_exists='replace',index=False)


