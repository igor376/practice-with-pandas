import pandas as pd
import sqlite3
import numpy as np
from re import findall


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
            if type(file_df.iat[x, y]) is not np.int64 and not file_df.iat[x, y].isdigit():
                corrected_cells += 1
                file_df.iat[x, y] = "".join(findall(r"[0-9]*", file_df.iat[x, y]))
    print(f'{corrected_cells} cell{was_or_were(corrected_cells)} corrected in {file_name}')
    file_df.to_csv(file_name, index=False)


def csv_to_db():
    global file_name
    db_name = file_name.replace("[CHECKED].csv", ".s3db")
    connection = sqlite3.Connection(f'{db_name}')
    file_df = pd.read_csv(file_name)
    file_df["score"] = calculate_points(file_df['engine_capacity'], file_df['fuel_consumption'],
                                        file_df['maximum_load'])
    # file_df.assign(
    #     score=calculate_points(file_df['engine_capacity'], file_df['fuel_consumption'], file_df['maximum_load']))
    columns = file_df.columns
    columns = {}.fromkeys(columns, "INTEGER NOT NULL")
    columns['vehicle_id'] = 'INTEGER PRIMARY KEY'
    file_df.to_sql("convoy", connection, if_exists="replace", index=False, dtype=columns)
    connection.commit()
    connection.close()
    print(f'{file_df.shape[0]} record{was_or_were(file_df.shape[0])} inserted into {db_name}')
    file_name = file_name.replace("[CHECKED].csv", ".s3db")


def db_to_json():
    global file_name
    file_df = get_filtered_items()
    file_name = file_name.replace(".s3db", ".json")
    with open(f'{file_name}', 'w') as in_file:
        in_file.write('{"convoy":')
        in_file.write(file_df.to_json(orient="records"))
        in_file.write('}')
    print(f'{file_df.shape[0]} vehicle{was_or_were(file_df.shape[0])} saved into {file_name}')
    file_name = file_name.replace(".json", ".s3db")


def db_to_xml():
    global file_name
    file_df = get_filtered_items(more_than_three=False)
    file_name = file_name.replace(".s3db", ".xml")
    if file_df.shape[0] == 0:
        with open(file_name, "w") as out_file:
            out_file.write("<convoy>\n</convoy>")
    else:
        file_df.to_xml(file_name, index=False, root_name="convoy", row_name="vehicle", xml_declaration=False)
    print(f'{file_df.shape[0]} vehicle{was_or_were(file_df.shape[0])} saved into {file_name}')


def was_or_were(number):
    if number == 1:
        return " was"
    else:
        return "s were"


def calculate_points(tank_capacity, fuel_consumption, maximum_load):
    supply_of_fuel = tank_capacity / fuel_consumption * 100
    points = np.where(supply_of_fuel >= 450, 2, np.where(supply_of_fuel >= 225, 1, 0))
    points += np.where((4.5 * fuel_consumption) <= 230, 2, 1)
    points += np.where(maximum_load >= 20, 2, 0)
    return points


def get_filtered_items(more_than_three=True):
    file_df = pd.read_sql_table("convoy", f'sqlite:///{file_name}')
    if more_than_three:
        file_df = file_df[file_df.score > 3]
    else:
        file_df = file_df[file_df.score <= 3]
    file_df.drop('score', axis=1, inplace=True)
    return file_df


file_name = input("Input file name\n")
if ".xlsx" in file_name:
    xlsx_to_csv()
    fix_csv()
    csv_to_db()
    db_to_json()
    db_to_xml()
elif "[CHECKED].csv" in file_name:
    csv_to_db()
    db_to_json()
    db_to_xml()
elif ".csv" in file_name:
    fix_csv()
    csv_to_db()
    db_to_json()
    db_to_xml()
elif ".s3db" in file_name:
    db_to_json()
    db_to_xml()
