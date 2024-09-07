import pyodbc
import numpy as np
import pandas as pd

class BigQuery:
    
    string_connection_parts = {
        'driver': "DRIVER={Simba ODBC Driver for Google BigQuery}"
        , 'catalog': "CATALOG={catalog}"
        , 'oauth_mechanism': "OAuthMechanism=1"
        , 'ignore_transactions': "IgnoreTransactions=1"
        , 'refresh_token': "RefreshToken={refresh_token}"
    }
    
    catalog:str = None
    refresh_token:str = None
    
    conn = None
    cur = None
    
    def __init__(self, catalog:str, refresh_token:str):
        self.catalog = catalog
        self.refresh_token = refresh_token
    
    def build_string_connection(self, catalog:str=None, refresh_token:str=None):
        if not catalog:
            catalog = self.catalog
        if not refresh_token:
            refresh_token = self.refresh_token
        
        str_conn_part = [
            self.string_connection_parts['driver']
            , self.string_connection_parts['catalog'].format(catalog=catalog)
            , self.string_connection_parts['oauth_mechanism']
            , self.string_connection_parts['ignore_transactions']
            , self.string_connection_parts['refresh_token'].format(refresh_token=refresh_token)
        ]
            
        return ';'.join(str_conn_part)
    
    def connection(self, catalog:str=None, refresh_token:str=None):
        self.conn = pyodbc.connect(
            self.build_string_connection(catalog, refresh_token)
        )
        
    def check_connection(self, catalog:str=None, refresh_token:str=None):
        if not self.conn:
            self.connection(catalog, refresh_token)
            
    def get_cursor(self, catalog:str=None, refresh_token:str=None):
        self.check_connection(catalog, refresh_token)
        self.cur = self.conn.cursor()
        
    def check_cursor(self, catalog:str=None, refresh_token:str=None):
        if not self.cur:
            self.get_cursor(catalog, refresh_token)
            
    def execute_query(self, query, commit=False, catalog:str=None, refresh_token:str=None):          
        self.check_cursor(catalog, refresh_token)
        res = self.cur.execute(query)
        if commit:
            self.commit()
            self.close()
        return res
    
    def select(self, query, frame=True, catalog:str=None, refresh_token:str=None):
        resp = self.execute_query(query, False, catalog, refresh_token)
        if frame:
            df = self.to_frame(resp)
            self.close()
            return df
        else:
            return resp
    
    def commit(self):
        self.conn.commit()
        
    def close(self):
        self.conn.close()
        self.conn = None
        self.cur = None
        
    def to_frame(self, resp):
        result = resp.fetchall()
        return pd.DataFrame(
            [list(l) for l in result]
            , columns = np.array(resp.description)[:,0]
        )
    
    def insert_many(self, sql, parms, catalog:str=None, refresh_token:str=None):
        self.check_cursor(catalog, refresh_token)
        self.cur.fast_executemany = True
        res = self.cur.executemany(sql, parms)
        self.commit()
        self.close()
