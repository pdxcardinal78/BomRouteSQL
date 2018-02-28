import pyodbc
import pandas as pd
from time import gmtime, strftime
import os
import private_key as pk

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=sql-prod;"
                      "Database=CompanyH;"
                      "Trusted_Connection=yes;"
                      "uid=pk.Private_Key_User_Name;"
                      "pwd=pk.Private_Key_User_Password")


df = pd.read_sql_query("select t1.StockCode, StockCodeDescription, Route, Operation, WorkCentre, AutoNarrCode, NarrationNum, Line, Narration \
                       from BomOperations as t1 \
                       join BomNarration \
                       on AutoNarrCode = NarrationNum \
                       join InvMaster as t2 \
                       on t1.StockCode = t2.StockCode \
                       Where WorkCentre = 'ALTMIL' \
                       ORDER BY AutoNarrCode, Line ASC", cnxn)

df_new = df.AutoNarrCode.value_counts().reset_index().rename(columns={'index': 'NarrationCode', 0: 'Counts'})
df_complete = pd.DataFrame(columns=('StockCode', 'Route', 'Operation', 'WorkCentre', 'Narration'))

for index, row in df_new.iterrows():
    narration_complete = None
    new = row['NarrationCode']
    df_filter = df[(df.AutoNarrCode == new)]
    for index, row2 in df_filter.iterrows():
        if narration_complete is None:
            narration_complete = row2['Narration']
        else:
            narration_complete = narration_complete + row2['Narration']
    df_complete = df_complete.append({'StockCode': row2['StockCode'], 'Route': row2['Route'],
                                      'Operation': row2['Operation'], 'WorkCentre': row2['WorkCentre'],
                                      'Narration': narration_complete}, ignore_index=True)

df_complete.to_csv(('test ' + strftime("%Y-%m-%d %H_%M_%S", gmtime()) + '.csv'), sep='\t')
