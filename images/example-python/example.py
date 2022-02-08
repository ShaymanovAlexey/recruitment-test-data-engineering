#!/usr/bin/env python

import csv
import json
import sqlalchemy
import pandas as pd

# connect to the database
engine = sqlalchemy.create_engine("mysql://codetest:swordfish@database/codetest")
connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)

# make an ORM object to refer to the table
Example = sqlalchemy.schema.Table('examples', metadata, autoload=True, autoload_with=engine)
Places = sqlalchemy.schema.Table('places', metadata, autoload=True, autoload_with=engine)
print(*connection.execute(sqlalchemy.sql.select([Places])).keys())

def generate_out_file(table, out_name):
  # output the table to a JSON file
  print("write report")
  columns = connection.execute(sqlalchemy.sql.select([Places])).keys()
  with open(out_name, 'w') as json_file:
    rows = connection.execute(sqlalchemy.sql.select([table])).fetchall()
    rows = [dict(zip(columns, row)) for row in rows]
    json.dump(rows, json_file, separators=(',', ':'))

def write_to_sql_table(data_in, table):
  print("write to table")
  df = pd.read_csv(data_in)
  print(df)
  with open(data_in) as csv_file:
    reader = csv.reader(csv_file)
    next(reader)
    for row in reader:
      connection.execute(table.insert().values(**dict(zip(df.columns, row))))

write_to_sql_table('/data/places.csv', Places)
write_to_sql_table('/data/example.csv', Example)
generate_out_file(Places, '/data/places_json.json')
# read the CSV data file into the table

