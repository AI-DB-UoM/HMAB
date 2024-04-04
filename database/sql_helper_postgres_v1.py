import configparser
import copy
import datetime
import logging
from collections import defaultdict

import datetime
import psycopg2
import psycopg2.extras

import constants
from database.column import Column
from database.qplan.pgread import PGReadQueryPlan
from database.table import Table

db_config = configparser.ConfigParser()
db_config.read(constants.DB_CONFIG)
db_type = db_config['SYSTEM']['db_type']
database = db_config[db_type]['database']

table_scan_times_hyp = copy.deepcopy(constants.TABLE_SCAN_TIMES[database])
table_scan_times = copy.deepcopy(constants.TABLE_SCAN_TIMES[database])
tables_global = None
pk_columns_dict = {}
count_numbers = {}
cache_hits = 0


# ############################# TA functions #############################


def create_index_v2(connection, query):
    """
    Create an index on the given table

    :param connection: sql_connection
    :param query: query for index creation
    """
    cursor = connection.cursor()
    t1 = datetime.datetime.now()
    cursor.execute(query)
    t2 = datetime.datetime.now()
    connection.commit()

    # Return the current reward
    return t2 - t1


def create_statistics(connection, query):
    """
    Create an index on the given table

    :param connection: sql_connection
    :param query: query for index creation
    """
    cursor = connection.cursor()
    start_time_execute = datetime.datetime.now()
    cursor.execute(query)
    connection.commit()
    end_time_execute = datetime.datetime.now()
    time_apply = (end_time_execute - start_time_execute).total_seconds()

    # Return the current reward
    return time_apply


def simple_execute(connection, query):
    """
    Drops the index on the given table with given name

    :param connection: sql_connection
    :param query: query to execute
    :return:
    """
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    logging.debug(query)


# ############################# Core MAB functions ##############################

def drop_v7(connection, schema_name, arm_list_to_delete):
    bulk_drop(connection, schema_name, arm_list_to_delete)
    

def bulk_drop(connection, schema_name, bandit_arm_list, file=None):
    """
    Drops the index for all given bandit arms

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param bandit_arm_list: list of bandit arms
    :return:
    """
    for name, bandit_arm in bandit_arm_list.items():
        if type(bandit_arm).__name__ == 'BanditArmMV':
            drop_view(connection, name)
        else:
            drop_index(connection,  bandit_arm.index_name)


def create_query_drop_v5(connection, schema_name, arm_list_to_add, arm_list_to_delete, queries):
    """
    This method aggregate few functions of the sql helper class.
        1. This method create the indexes related to the given bandit arms
        2. Execute all the queries in the given list
        3. Clean (drop) the created indexes
        4. Finally returns the cost taken to run all the queries

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param arm_list_to_add: arms that need to be added in this round
    :param arm_list_to_delete: arms that need to be removed in this round
    :param queries: queries that should be executed
    :return:
    """
    if tables_global is None:
        get_tables(connection)
    bulk_drop_index(connection, schema_name, arm_list_to_delete)
    creation_cost = bulk_create_indexes(connection, arm_list_to_add)
    execute_cost = 0
    execute_cost_transactional = 0
    execute_cost_analytical = 0
    query_plans = []
    for query in queries:
        query_plan = execute_query_v2(connection, query.get_query_string())
        if query_plan:
            cost = query_plan[constants.COST_TYPE_CURRENT_EXECUTION]
            if cost > 0.5:
                logging.info(f"Query {query.id} cost: {cost}")
            execute_cost += cost
            if query.is_analytical:
                execute_cost_analytical += cost
            else:
                execute_cost_transactional += cost
            if query.first_seen == query.last_seen:
                query.original_running_time = cost

        query_plans.append(query_plan)

    logging.info(f"Index creation cost: {sum(creation_cost.values())}")
    logging.info(f"Time taken to run the queries: {execute_cost}")
    logging.info(f"Time taken for analytical queries: {execute_cost_analytical}")
    logging.info(f"Time taken for transactional queries: {execute_cost_transactional}")

    return execute_cost, creation_cost, query_plans, execute_cost_analytical, execute_cost_transactional


def bulk_create_indexes(connection, bandit_arm_list):
    """
    This uses create_index method to create multiple indexes at once. This is used when a super arm is pulled

    :param connection: sql_connection
    :param bandit_arm_list: list of BanditArm objects
    :return: cost (regret)
    """
    cost = {}
    for index_name, bandit_arm in bandit_arm_list.items():
        cost[index_name] = create_index_v1(connection, bandit_arm.table_name, bandit_arm.index_cols, bandit_arm.index_name,
                                           bandit_arm.include_cols)
        set_arm_size(connection, bandit_arm)
    return cost


def bulk_drop_index(connection, schema_name, bandit_arm_list):
    """
    Drops the index for all given bandit arms

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param bandit_arm_list: list of bandit arms
    :return:
    """
    for index_name, bandit_arm in bandit_arm_list.items():
        drop_index(connection, bandit_arm.index_name)


def create_index_v1(connection, tbl_name, col_names, idx_name, include_cols=()):
    """
    Create an index on the given table

    :param connection: sql_connection
    :param tbl_name: name of the database table
    :param col_names: string list of column names
    :param idx_name: name of the index
    :param include_cols: columns that needed to added as includes
    """
    if include_cols:
        query = f"CREATE INDEX {idx_name} ON {tbl_name} ({', '.join(list(col_names) + include_cols)})"
    else:
        query = f"CREATE INDEX {idx_name} ON {tbl_name} ({', '.join(col_names)})"
    cursor = connection.cursor()
    t1 = datetime.datetime.now()
    cursor.execute(query)
    t2 = datetime.datetime.now()
    connection.commit()
    logging.info(f"Added: {idx_name}")
    logging.debug(query)

    # Return the current reward
    return (t2 - t1).seconds


def drop_index(connection, idx_name):
    """
    Drops the index on the given table with given name

    :param connection: sql_connection
    :param idx_name: name of the index
    :return:
    """
    query = f"DROP INDEX {idx_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    logging.info(f"removed: {idx_name}")
    logging.debug(query)


def drop_view(connection, view_name):
    """
    Drops the view with the given name if it exists.

    :param connection: sql_connection to the database.
    :param view_name: The name of the view to drop.
    :return: None
    """
    query = f"DROP VIEW IF EXISTS {view_name}"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        logging.info(f"View removed: {view_name}")
    except Exception as e:
        # Rollback in case of error
        connection.rollback()
        logging.error(f"Error removing view {view_name}: {e}")
    finally:
        # Ensuring the cursor is closed after operation
        cursor.close()
    logging.debug(query)



def execute_query_v2(connection, query, print_exc=True):
    """
    This executes the given query and return the time took to run the query. This Clears the cache and executes
    the query and return the time taken to run the query. This return the 'elapsed time' by default.
    However its possible to get the cpu time by setting the is_cpu_time to True

    :param connection: sql_connection
    :param query: query that need to be executed
    :param print_exc: print the exception, True or False
    :return: time taken for the query
    """
    try:
        cursor = connection.cursor()
        prefix = "EXPLAIN (ANALYZE TRUE, VERBOSE TRUE, COSTS TRUE, BUFFERS TRUE, FORMAT XML) "
        cursor.execute(prefix + query)
        stat_xml = cursor.fetchone()[0]
        connection.commit()
        return PGReadQueryPlan(stat_xml)
    except Exception as e:
        if print_exc:
            print("Exception when executing query: ", query)
            print(e)
        return None




# ############################# Hyp MAB functions ##############################

def hyp_check_config(connection, schema_name, arm_list_to_add, queries, file_path):
    """
    This method aggregate few functions of the sql helper class.
        1. This method create the indexes related to the given bandit arms
        2. Execute all the queries in the given list
        3. Clean (drop) the created indexes
        4. Finally returns the time taken to run all the queries

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param arm_list_to_add: new arms considered in this round
    :param super_arm_list: complete arm list
    :param queries: queries that should be executed
    :return:
    """
    cost = 0
    file = open(file_path, 'w')
    if tables_global is None:
        get_tables(connection)
    cost += hyp_bulk_create(connection, schema_name, arm_list_to_add, file)
    query_plans = []
    hyp_enable_index(connection, file)
    for query in queries:
        query_plan, exe_cost = hyp_execute_query_v2(connection, query.get_query_string(hyp=True), file)
        query_plans.append(query_plan)
        cost += exe_cost
        if query.first_seen == query.last_seen:
            query.original_hyp_running_time = query_plan.sub_tree_cost
    bulk_drop(connection, schema_name, arm_list_to_add, file)
    file.close()

    return query_plans, cost



def hyp_enable_index(connection, file):
    """
    This enables the hypothetical indexes for the given connection. This will be enabled for a given connection and all
    hypothetical queries must be executed via the same connection
    :param connection: connection for which hypothetical indexes will be enabled
    """
    pass
    # query = f'''SELECT dbid = Db_id(),
    #                 objectid = object_id,
    #                 indid = index_id, type
    #             FROM   sys.indexes
    #             WHERE  is_hypothetical = 1;'''
    # cursor = connection.cursor()
    # cursor.execute(query)
    # result_rows = cursor.fetchall()
    # for result_row in result_rows:
    #     if result_row[3] == 2:
    #         query_2 = f"DBCC AUTOPILOT(0, {result_row[0]}, {result_row[1]}, {result_row[2]})"
    #         cursor.execute(query_2)
    #         file.write(query_2 + ';\n')
    #     elif result_row[3] == 1:
    #         query_2 = f"DBCC AUTOPILOT(6, {result_row[0]}, {result_row[1]}, {result_row[2]})"
    #         cursor.execute(query_2)
    #         file.write(query_2 + ';\n')
    #     else:
    #         print('Unknown index type')


def hyp_bulk_create(connection, schema_name, bandit_arm_list, file):
    """
        This uses create_index method to create multiple indexes at once. This is used when a super arm is pulled

        :param connection: sql_connection
        :param schema_name: name of the database schema
        :param bandit_arm_list: list of BanditArm objects
        :return: cost (regret)
    """
    cost = 0
    for name, bandit_arm in bandit_arm_list.items():
        if type(bandit_arm).__name__ == 'BanditArmMV':
            cost += hyp_create_view(connection, bandit_arm.index_name, bandit_arm.view_query,
                                         bandit_arm.index_query, file)
        else:
            cost += hyp_create_index_v1(connection, schema_name, bandit_arm.table_name, bandit_arm.index_cols,
                                             bandit_arm.index_name, file, bandit_arm.include_cols)
    return cost



def hyp_create_view(connection, view_name, view_query, index_query, file):
    # Initial view creation operation has a negligible cost. So the returned cost is from the clustered index creation.

    # creates the view
    cursor = connection.cursor()
    cursor.execute(view_query)
    connection.commit()

    # creates the clustered index
    cursor.execute("SET STATISTICS XML ON")
    start_time = datetime.datetime.now()
    cursor.execute(index_query[:-1] + " WITH STATISTICS_ONLY = -1;")
    file.write(index_query[:-1] + " WITH STATISTICS_ONLY = -1;\n")
    end_time = datetime.datetime.now()
    cursor.execute("SET STATISTICS XML OFF")
    connection.commit()
    logging.info(f"Added and Created Hyp Clustered Index on: {view_name}")

    return (end_time - start_time).total_seconds()


def hyp_execute_query_v2(connection, query, file, print_exc=True):
    """
    This executes the given query in PostgreSQL and returns the time taken to run the query. It optionally clears the
    cache (if running as a superuser) and captures the query plan using EXPLAIN (ANALYZE, BUFFERS, FORMAT XML).
    Note: Clearing cache in PostgreSQL requires superuser access and is not recommended in production environments.

    :param connection: psycopg2 connection to the PostgreSQL database
    :param query: SQL query to be executed
    :param file: A file object to write the executed query to
    :param print_exc: Whether to print exception messages
    :return: Execution plan as XML and time taken for the query in seconds
    """
    try:
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Optionally, clear cache by running on a separate superuser session:
        # connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        # cursor.execute("SELECT pg_backend_pid();")
        # pid = cursor.fetchone()[0]
        # cursor.execute(f"SELECT pg_terminate_backend({pid});")
        # connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT XML) {query}"
        start_time = datetime.datetime.now()
        cursor.execute(explain_query)
        end_time = datetime.datetime.now()
        
        file.write(query + '\n')
        execution_time = (end_time - start_time).total_seconds()
        
        # Fetching the XML plan generated by EXPLAIN
        stat_xml = cursor.fetchone()[0]
        
        connection.commit()
        cursor.close()
        return stat_xml, execution_time
    except Exception as e:
        if print_exc:
            print(f"Exception occurred: {e}")
        return None, execution_time
    

    # ############################# Helper function ##############################

def get_table_row_count(connection, tbl_name):
    row_query = f"SELECT reltuples as approximate_row_count FROM pg_class WHERE relname = '{tbl_name}';"
    cursor = connection.cursor()
    cursor.execute(row_query)
    row_count = cursor.fetchone()[0]
    return row_count


def get_all_columns(connection):
    """
    Get all column in the database of the given connection. Note that the connection here is directly pointing to a
    specific database of interest

    :param connection: Sql connection
    :return: dictionary of lists - columns, number of columns
    """
    query = """SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='public'"""
    columns = defaultdict(list)
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    for result in results:
        columns[result[0]].append(result[1])

    return columns, len(results)


def get_current_pds_size(connection):
    """
    Get the current size of all the physical design structures
    :param connection: SQL Connection
    :return: size of all the physical design structures in MB
    """

    query = '''SELECT SUM(pg_indexes_size(relid))/(1024 * 1024) AS size_mb
                FROM pg_catalog.pg_statio_user_tables;'''

    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    print(result)
    return result[0]


def get_primary_key(connection, table_name):
    """
    Get Primary key of a given table. Note tis might not be in order (not sure)
    :param connection: SQL Connection
    :param schema_name: schema name of table
    :param table_name: table name which we want to find the PK
    :return: array of columns
    """
    if table_name in pk_columns_dict:
        pk_columns = pk_columns_dict[table_name]
    else:
        pk_columns = []
        query = f"""SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
                    FROM   pg_index i
                    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                         AND a.attnum = ANY(i.indkey)
                    WHERE  i.indrelid = '{table_name}'::regclass
                    AND    i.indisprimary;"""
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        for result in results:
            pk_columns.append(result[0])
        pk_columns_dict[table_name] = pk_columns
    return pk_columns


def get_column_data_length_v2(connection, table_name, col_names):
    """
    get the data length of given set of columns
    :param connection: SQL Connection
    :param table_name: Name of the SQL table
    :param col_names: array of columns
    :return:
    """
    tables = get_tables(connection)
    column_data_length = 0

    for column_name in col_names:
        column = tables[table_name].columns[column_name]
        column_data_length += column.column_size if column.column_size else 0

    return column_data_length

def get_table_columns(connection, table_name):
    """
    Get all the columns in the given table

    :param connection: sql connection
    :param table_name: table name
    :return: dictionary of columns column name as the key
    """
    columns = {}
    cursor = connection.cursor()
    data_type_query = f"""SELECT column_name, data_type, pg_column_size(data_type)
                          FROM information_schema.columns
                         WHERE table_schema = 'public'
                           AND table_name   = '{table_name}';"""
    cursor.execute(data_type_query)
    results = cursor.fetchall()
    for result in results:
        col_name = result[0]
        column = Column(table_name, col_name, result[1])
        column.set_column_size(int(result[2]))
        columns[col_name] = column

    return columns, len(results)


def get_columns(connection, table_name):
    """
    Get all the columns in the given table

    :param connection: sql connection
    :param table_name: table name
    :return: dictionary of columns column name as the key
    """
    columns = {}
    cursor = connection.cursor()
    data_type_query = f"""SELECT column_name, data_type, pg_column_size(data_type)
                          FROM information_schema.columns
                         WHERE table_schema = 'public'
                           AND table_name   = '{table_name}';"""
    cursor.execute(data_type_query)
    results = cursor.fetchall()
    for result in results:
        col_name = result[0]
        column = Column(table_name, col_name, result[1])
        column.set_column_size(int(result[2]))
        columns[col_name] = column

    return columns


def get_tables(connection):
    """
    Get all tables as Table objects
    :param connection: SQL Connection
    :return: Table dictionary with table name as the key
    """
    global tables_global
    if tables_global is not None:
        return tables_global
    else:
        tables = {}
        get_tables_query = """select table_name from information_schema.tables
                                where table_schema = 'public' and table_type='BASE TABLE'"""
        cursor = connection.cursor()
        cursor.execute(get_tables_query)
        results = cursor.fetchall()
        for result in results:
            table_name = result[0]
            row_count = get_table_row_count(connection, table_name)
            pk_columns = get_primary_key(connection, table_name)
            tables[table_name] = Table(table_name, row_count, pk_columns)
            tables[table_name].set_columns(get_columns(connection, table_name))
        tables_global= tables
    return tables_global

def get_table_list(connection, min_row_count):

    get_tables(connection)

    table_name_list = []
    for table_name, table in tables_global.items():
        if table.table_row_count > min_row_count:
            table_name_list.append(table_name)
    return table_name_list


def get_estimated_size_of_index_v1(connection, schema_name, tbl_name, col_names):
    """
    This helper method can be used to get a estimate size for a index. This simply multiply the column sizes with a
    estimated row count (need to improve further)

    :param connection: sql_connection
    :param schema_name: name of the database schema
    :param tbl_name: name of the database table
    :param col_names: string list of column names
    :return: estimated size in MB
    """
    table = get_tables(connection)[tbl_name]
    primary_key = get_primary_key(connection, tbl_name)
    col_not_pk = tuple(set(col_names) - set(primary_key))
    key_columns_length = get_column_data_length_v2(connection, tbl_name, col_not_pk)
    row_count = table.table_row_count
    estimated_size = row_count * key_columns_length
    estimated_size = estimated_size/float(1024*1024)
    return estimated_size


def get_query_plan(connection, query):
    """
    This returns the XML query plan of  the given query

    :param connection: sql_connection
    :param query: sql query for which we need the query plan
    :return: XML query plan as a String
    """
    try:
        cursor = connection.cursor()
        prefix = "EXPLAIN (VERBOSE TRUE, COSTS TRUE, BUFFERS TRUE, FORMAT XML) "
        cursor.execute(prefix + query)
        stat_xml = cursor.fetchone()[0]
        connection.commit()
        return PGReadQueryPlan(stat_xml)
    except Exception as e:
        print("Exception when executing query: ", query)
        return None


def get_selectivity_v3(connection, query, predicates):
    """
    Return the selectivity of the given query

    :param connection: sql connection
    :param query: sql query for which predicates will be identified
    :param predicates: predicates of that query
    :return: Predicates list
    """

    selectivity = {}
    tables = predicates.keys()
    for table in tables:
        selectivity[table] = 0.001

    return selectivity


def remove_all_non_clustered(connection):
    """
    Removes all non-clustered indexes from the database
    :param connection: SQL Connection
    """
    query = """SELECT
                 c_ind.relname
                  FROM pg_index ind
                  JOIN pg_class c_ind ON c_ind.oid = ind.indexrelid
                  JOIN pg_namespace n ON n.oid = c_ind.relnamespace
                  LEFT JOIN pg_constraint cons ON cons.conindid = ind.indexrelid
                  WHERE
                n.nspname NOT IN ('pg_catalog','information_schema') AND 
                n.nspname !~ '^pg_toast'::TEXT AND
                cons.oid IS NULL"""
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    for result in results:
            drop_index(connection, result[0])


def get_table_scan_times_structure():
    # query_table_scan_times = copy.deepcopy(constants.TABLE_SCAN_TIMES[database[:-4]])
    query_table_scan_times = copy.deepcopy(constants.TABLE_SCAN_TIMES[database])
    return query_table_scan_times


def drop_all_dta_statistics(connection):
    # query_get_stat_names = """SELECT OBJECT_NAME(s.[object_id]) AS TableName, s.[name] AS StatName
    #                             FROM sys.stats s
    #                             WHERE OBJECTPROPERTY(s.OBJECT_ID,'IsUserTable') = 1 AND s.name LIKE '_dta_stat%';"""
    # cursor = connection.cursor()
    # cursor.execute(query_get_stat_names)
    # results = cursor.fetchall()
    # for result in results:
    #     drop_statistic(connection, result[0], result[1])
    logging.info("Dropped all dta statistics")


# def drop_statistic(connection, table_name, stat_name):
#     query = f"DROP STATISTICS {table_name}.{stat_name}"
#     cursor = connection.cursor()
#     cursor.execute(query)
#     cursor.commit()


def set_arm_size(connection, bandit_arm):
    query = f"select CAST(pg_table_size('{bandit_arm.index_name}') as float)/(1024*1024);"
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    bandit_arm.memory = result[0]
    return bandit_arm


# def restart_sql_server():
#     command1 = f"net stop mssqlserver"
#     command2 = f"net start mssqlserver"
#     with open(os.devnull, 'w') as devnull:
#         subprocess.run(command1, shell=True, stdout=devnull)
#         time.sleep(45)
#         subprocess.run(command2, shell=True, stdout=devnull)
#         time.sleep(15)
#     logging.info("Server Restarted")
#     return


# def get_last_restart_time(connection):
#     query_get_stat_names = "SELECT sqlserver_start_time FROM sys.dm_os_sys_info;"
#     cursor = connection.cursor()
#     cursor.execute(query_get_stat_names)
#     result = cursor.fetchone()
#     logging.info("Last Server Restart Time: " + str(result[0]))
#     return result[0]


def get_database_size(connection):
    database_size = 10240
    try:
        query = f"SELECT pg_size_pretty (pg_database_size ('{database}'));"
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        database_size = float(result[0].split(" ")[0])/1024
    except Exception as e:
        logging.error("Exception when get_database_size: " + str(e))
    return database_size


def clean_up_routine(sql_connection):
    # restart server. We need to do this before restore to remove all connections
    # if constants.SERVER_RESTART:
    #     restart_sql_server()
    #
    master_connection = sql_connection.get_sql_connection()
    #
    # # restore the backup
    # if constants.RESTORE_BACKUP:
    #     restore_database_snapshot(master_connection)

    # sql_connection.close_sql_connection(master_connection)
    remove_all_non_clustered(master_connection)


def create_database_snapshot(master_connection):
    """
    This create ta database snapshot, we need to create a snapshot when we setup experiment
    local: C:\Program Files\Microsoft SQL Server\MSSQL14.MSSQLSERVER\MSSQL\Backup
    server: default

    :param master_connection: connection to the master DB
    """
    # ss_name = f"{database}_snapshot"
    # ss_location = "E:\Microsoft SQL Server Data\MSSQL14.MSSQLSERVER\MSSQL\Backup"
    # create_ss_query = f"""CREATE DATABASE {ss_name} ON
    #                     ( NAME = {database}, FILENAME =
    #                     '{ss_location}\\{ss_name}.ss' )
    #                     AS SNAPSHOT OF {database};
    #                     """
    # cursor = master_connection.cursor()
    # cursor.execute(create_ss_query)
    # while cursor.nextset():
    #     pass
    # logging.info("DB snapshot created")
    # print(create_ss_query)
    return None


def restore_database_snapshot(master_connection):
    """
    This restores the database snapshot, we have to make sure there is only one snapshot at a time.

    :param master_connection: connection to the master DB
    """
    # ss_name = f"{database}_snapshot"
    # restore_ss_query = f"""RESTORE DATABASE {database} from
    #                     DATABASE_SNAPSHOT = '{ss_name}';
    #                     """
    # cursor = master_connection.cursor()
    # cursor.execute(restore_ss_query)
    # while cursor.nextset():
    #     pass
    # logging.info("DB snapshot restored")
    return None
