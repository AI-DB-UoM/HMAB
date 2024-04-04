import psycopg2
from psycopg2 import OperationalError

import mysql.connector
from mysql.connector import Error

import pymssql
import csv
import pyodbc


def create_tables_mssql():
    try:
        connection = pymssql.connect(server='localhost',
                                    user='SA',
                                    password='Lgl125!!',
                                    database='TPCDS',
                                    port='1434')

        cursor = connection.cursor()

        database_name = 'TPCDS'
        cursor.execute("""
            SELECT name 
            FROM sys.databases 
            WHERE name = %s
        """, (database_name,))
        result = cursor.fetchone()

        if result:
            sql_script_path = '/Users/guanlil1/Dropbox/PostDoc/topics/MAB/DSGen-software-code-3.2.0rc1/tools/tpcds.sql'
            with open(sql_script_path, 'r') as file:
                sql_script = file.read()
                cursor.execute(sql_script)
                connection.commit()
        else:
            print(f"Database {database_name} does not exist. Nee to create first.")
            return

        cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        # connection.commit()
        for row in cursor:
            print("Schema: {}, Table: {}".format(row[0], row[1]))
    except pymssql.Error as db_err:
        print("Database error:", db_err)
        
    except Exception as e:
        print("General error:", e)

    finally:
        if connection:
            connection.close()
            print("SQL Server connection is closed")

def create_tables_pg():
    try:
        connection = psycopg2.connect(
                    host='localhost',
                    database='tpcds',
                    user='postgres',
                    password='123456',
                    port='5433') 

        cursor = connection.cursor()

        database_name = 'tpcds'
        cursor.execute("SELECT datname FROM pg_database WHERE datname = %s", (database_name,))
        result = cursor.fetchone()

        if result:
            sql_script_path = '/Users/guanlil1/Dropbox/PostDoc/topics/MAB/DSGen-software-code-3.2.0rc1/tools/tpcds.sql'
            with open(sql_script_path, 'r') as file:
                sql_script = file.read()
                cursor.execute(sql_script)
                connection.commit()
        else:
            print(f"Database {database_name} does not exist. Need to create first.")
            return

        cursor.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')")
        for row in cursor:
            print("Schema: {}, Table: {}".format(row[0], row[1]))

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")

    finally:
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")

def insert_data_mssql():
    try:
        connection = pymssql.connect(server='localhost',
                                    user='SA',
                                    password='Lgl125!!',
                                    database='TPCDS',
                                    port='1434')

        cursor = connection.cursor()
        cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row[1] for row in cursor]

    
        for table in tables:
            
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
    
            # with open(f'/Users/guanlil1/Dropbox/PostDoc/topics/MAB/DSGen-software-code-3.2.0rc1/tools/data/{table}.dat', 'r') as file:
            #     for _ in range(5):  # 仅打印前5行作为示例
            #         line = file.readline()
            #         print(line, end='')  # 打印行内容，`end=''` 防止额外的新行


            print("Loading table {} ......".format(table))

            if row_count > 0:
                print("Table {} was loaded".format(table))
                continue

            try:
                bulk_insert_cmd = f"""
                BULK INSERT {table}
                FROM '/data/{table}.dat'
                WITH (
                    FIELDTERMINATOR = '|',
                    ROWTERMINATOR = '\n',
                    FIRSTROW = 1
                );
                """

                # Execute the BULK INSERT command
                cursor.execute(bulk_insert_cmd)
                connection.commit()
                print("Table {} is loaded".format(table))

            except pymssql.Error as db_err:
                print(f"Error loading table {table}: {db_err}")
            # connection.commit()
            print("Continue to next table")
            
    
        connection.commit() 

    except pymssql.Error as db_err:
        print("Database error:", db_err)
        
    except Exception as e:
        print("General error:", e)

    finally:
        if connection:
            connection.close()
            print("SQL Server connection is closed")



def convert_dat_to_csv(dat_file_path, csv_file_path, table_defaults, dat_delimiter='|', csv_delimiter=','):
    """
    Convert a .dat file to a .csv file.

    Parameters:
    - dat_file_path: Path to the input .dat file.
    - csv_file_path: Path where the output .csv file will be saved.
    - dat_delimiter: The delimiter used in the .dat file (default is '|').
    - csv_delimiter: The delimiter to be used in the .csv file (default is ',').
    """

    try:
        with open(dat_file_path, 'r', encoding='utf-8') as dat_file:
            with open(csv_file_path, 'w', encoding='utf-8', newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter=csv_delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL)
                # for line in dat_file:
                #     dat_fields = line.strip().split(dat_delimiter)
                #     # 使用默认值填充缺失的数据
                #     updated_fields = [field if field or field == '' else table_defaults[i] for i, field in enumerate(dat_fields)]
                #     # 限制updated_fields长度与defaults相同，防止数据字段超出预定义结构
                #     updated_fields = updated_fields[:len(table_defaults)]
                #     # 写入CSV文件
                #     writer.writerow(updated_fields)
                for j, line in enumerate(dat_file):
                    # if j > 0:
                    #     break
                    # Split the line by the dat file delimiter and join by the csv delimiter
                    # csv_line = csv_delimiter.join(line.strip().split(dat_delimiter))
                    # Write the transformed line to the csv file

                    line = line.replace(',', '.')

                    dat_fields = line.strip().split(dat_delimiter)

                    # print("len:", len(dat_fields))
                    # print(dat_fields)
            
                    # Prepare a list to hold the updated fields with default values where necessary
                    updated_fields = []
                    

                    is_updated = False
                    for i, field in enumerate(dat_fields):

                        if field == '' and i < len(table_defaults):
                            # is_updated = True
                            # break
                            # If the field is empty and a default value exists, use the default
                            updated_fields.append(str(table_defaults[i]))
                        else:
                            # Otherwise, use the field as is
                            updated_fields.append(field)

                    # if is_updated:
                    #     continue
                    # Join the updated fields by the csv delimiter
                    # csv_line = csv_delimiter.join(updated_fields)
                    # print(updated_fields)
                    # print("len:", len(updated_fields))

                    writer.writerow(updated_fields)

                    # csv_file.write(csv_line + '\n')
                    

        print(f"Successfully converted {dat_file_path} to {csv_file_path}.")
    except Exception as e:
        print(f"Error converting {dat_file_path} to {csv_file_path}: {e}")


def insert_data_pg(default_values):
    try:
        connection = psycopg2.connect(
                    host='localhost',
                    database='tpcds',
                    user='postgres',
                    password='123456',
                    port='5433') 

        cursor = connection.cursor()

        # 获取数据库中的所有表
        cursor.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND
            table_schema NOT IN ('pg_catalog', 'information_schema')
        """)
        tables = [row[1] for row in cursor.fetchall()]

        tables = ['call_center']

        for table in tables:
            # 检查表中的行数
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]

            print("Loading table {} ...... row_count:{}".format(table, row_count))

            if row_count > 0:
                print(f"Table {table} was loaded")
                continue

            # 用于示例的数据文件路径，根据实际情况调整
            data_file_path = f'/Users/guanlil1/Dropbox/PostDoc/topics/MAB/DSGen-software-code-3.2.0rc1/tools/data/{table}'  # 请根据实际情况调整文件路径和扩展名

            convert_dat_to_csv(data_file_path + ".dat", data_file_path + ".csv", default_values[table])

            # 使用COPY命令从文件加载数据
            try:
                with open(data_file_path+ ".csv", 'r', encoding='utf-8') as file:
                    cursor.copy_from(file, table, sep=',')  # 假设数据以'|'分隔
                connection.commit()
                print(f"Table {table} is loaded")

            except Exception as db_err:
                print(f"Error loading table {table}: {db_err}")
                connection.rollback()  # 如果出错，回滚
            print("Continue to next table")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")

    finally:
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")


def query_table_mssql():
    try:
        connection = pymssql.connect(server='localhost',
                                    user='SA',
                                    password='Lgl125!!',
                                    database='TPCDS',
                                    port='1434')

        cursor = connection.cursor()
        query = "SELECT * FROM customer_address"
        # 执行查询
        cursor.execute(query)

    # 获取查询结果
        results = cursor.fetchall()

        # 打印结果
        for row in results:
            print(row)
    except pymssql.Error as db_err:
        print("Database error:", db_err)
        
    except Exception as e:
        print("General error:", e)

    finally:
        if connection:
            connection.close()
            print("SQL Server connection is closed")

def export_data_mssql():
    try:
        # 连接到SQL Server
        connection = pymssql.connect(server='localhost',
                                     user='SA',
                                     password='Lgl125!!',
                                     database='TPCDS',
                                     port='1434')
        cursor = connection.cursor()

        # 获取所有基本表的列表
        cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row[1] for row in cursor]

        for table in tables:
            # 为每个表构造查询来获取其所有数据
            cursor.execute(f"SELECT * FROM {table}")

            # 获取查询结果
            rows = cursor.fetchall()

            # 指定导出文件的路径
            csv_file_path = f'/Users/guanlil1/Dropbox/PostDoc/topics/MAB/DSGen-software-code-3.2.0rc1/tools/data/{table}.csv'

            # 将数据写入到CSV文件中
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                
                # # 可选：写入列标题（如果你想要表头）
                # columns = [i[0] for i in cursor.description]
                # csv_writer.writerow(columns)

                # 写入数据
                for row in rows:
                    csv_writer.writerow(row)

            print(f"Table {table} exported to CSV successfully.")

    except pymssql.Error as db_err:
        print("Database error:", db_err)
        
    except Exception as e:
        print("General error:", e)

    finally:
        if connection:
            connection.close()
            print("SQL Server connection is closed")


default_values = {
    "dbgen_version": ["", None, None, ""],
    "customer_address": [0, "", "0", "", "", "", "", "", "", "", "", 0.0, ""],
    "customer_demographics": [0, "", "", "", 0, "", 0, 0, 0],
    "date_dim": [0, "", None, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", "", "", "", "", 0, 0, 0, 0, "", "", "", "", ""],
    "warehouse": [0, "", "", 0, "", "", "", "", "", "", "", "", "", 0.0],
    "ship_mode": [0, "", "", "", "", ""],
    "time_dim": [0, "", 0, 0, 0, 0, "", "", "", ""],
    "reason": [0, "", ""],
    "income_band": [0, 0, 0],
    "item": [0, "", None, None, "", 0.0, 0.0, 0, "", 0, "", 0, "", 0, "", "", "", "", "", "", 0, ""],
    "store": [0, "", None, None, None, "", 0, 0, "", "", 0, "", "", "", 0, "", 0, "", "", "", "", "", "", 0.0, 0.0],
    "call_center": [0, "", None, "2000-12-31", 0, 0, "", "", 0, 0, "", "", 0, "", "", "", 0, "", 0, "", "", "", "", "", "", "", "", "", "", 0.0, 0.0],
    "customer": [0, "", 0, 0, 0, 0, 0, "", "", "", "", 0, 0, 0, "", "", "", ""],
    "web_site": [0, "", None, None, "", None, None, "", "", 0, "", "", 0, "", "", "", "", "", "", "", "", "", "", 0.0, 0.0],
    "store_returns": [None, None, 0, None, None, None, None, None, None, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "household_demographics": [0, None, "", 0, 0],
    "web_page": [0, "", None, None, None, None, "", None, "", "", 0, 0, 0, 0],
    "promotion": [0, "", None, None, None, 0.0, 0, "", "", "", "", "", "", "", "", "", "", "", ""],
    "catalog_page": [0, "", None, None, "", 0, 0, "", ""],
    "inventory": [0, 0, 0, 0],
    "catalog_returns": [None, None, 0, None, None, None, None, None, None, None, None, None, None, None, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "web_returns": [None, None, 0, None, None, None, None, None, None, None, None, None, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "web_sales": [None, None, None, 0, None, None, None, None, None, None, None, None, None, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "catalog_sales": [None, None, None, None, None, None, None, None, None, None, None, None, 0, None, 0, 0, 0.0, 0.0, 0.0, 0.0, 0],
    "catalog_sales": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "store_sales": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
}

if __name__ == '__main__':
    # create_tables_mssql()
    # insert_data_mssql()
    # query_table_mssql()

    # create_tables_pg()
    # export_data_mssql()
    insert_data_pg(default_values)

