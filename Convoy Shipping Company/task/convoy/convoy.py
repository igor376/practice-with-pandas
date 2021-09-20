import pandas as pd
import sqlite3
from re import findall
from sqlalchemy import create_engine


def xlsx_to_csv():
    global file_name
    file_df = pd.read_excel(file_name, sheet_name="Vehicles", dtype=str)
    file_name = file_name.replace(".xlsx", ".csv")
    file_df.to_csv(file_name, index=False)
    print(f'{file_df.shape[0]} line{was_or_were(file_df.shape[0])} imported to {file_name}')


def fix_csv():
    global file_name
    file_df = pd.read_csv(file_name)
    file_name = file_name.replace(".csv", "[CHECKED].csv")
    corrected_cells = 0
    for x in range(file_df.shape[0]):
        for y in range(file_df.shape[1]):
            if not file_df.iat[x, y].isdigit():
                corrected_cells += 1
                file_df.iat[x, y] = "".join(findall(r"[0-9]*", file_df.iat[x, y]))
    print(f'{corrected_cells} cell{was_or_were(corrected_cells)} corrected in {file_name}')
    file_df.to_csv(file_name, index=False)


def csv_to_db():
    global file_name
    db_name = file_name.replace("[CHECKED].csv", ".s3db")
    connection = sqlite3.Connection(f'{db_name}')
    file_df = pd.read_csv(file_name)
    columns = file_df.columns
    columns = {}.fromkeys(columns, "INTEGER NOT NULL")
    columns['vehicle_id'] = 'INTEGER PRIMARY KEY'
    file_df.to_sql("convoy", connection, if_exists="replace", index=False, dtype=columns)
    connection.commit()
    connection.close()
    print(f'{file_df.shape[0]} record{was_or_were(file_df.shape[0])} inserted into {db_name}')
    file_name = file_name.replace("[CHECKED].csv", ".s3db")


def from_db_to_json():
    global file_name
    file_df = pd.read_sql_table("convoy", f'sqlite:///{file_name}')
    file_name = file_name.replace(".s3db", ".json")
    with open(f'{file_name}', 'w') as in_file:
        in_file.write('{"convoy":')
        in_file.write(file_df.to_json(orient="records"))
        in_file.write('}')
    print(f'{file_df.shape[0]} vehicle{was_or_were(file_df.shape[0])} saved into {file_name}')


def was_or_were(number):
    if number == 1:
        return " was"
    else:
        return "s were"


file_name = input("Input file name\n")
if ".xlsx" in file_name:
    xlsx_to_csv()
    fix_csv()
    csv_to_db()
    from_db_to_json()
elif "[CHECKED].csv" in file_name:
    csv_to_db()
    from_db_to_json()
elif ".csv" in file_name:
    fix_csv()
    csv_to_db()
    from_db_to_json()
elif ".s3db" in file_name:
    from_db_to_json()
