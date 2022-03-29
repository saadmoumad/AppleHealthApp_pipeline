import mysql.connector
from mysql.connector import Error

class MySQL_insertion():
    def __init__(self, mysql_file,):
        self.mysql_file = mysql_file
        self.connection = mysql.connector.connect(host='**db_host',
                                    database='Apple_health',
                                    user='**user_name',
                                    password='**user_password')

    def __pre_sql(self, table):
        sql_query = "INSERT INTO Health_data ("+ ', '.join(table.columns.tolist()) +") VALUES (%s, %s, %s, %s)"
        val = []
        for i,row in table.iterrows():
            val.append(tuple(row))
        return sql_query, val

    def get_files_names(self):
        if self.connection.is_connected():
            print("Connection succeeded!!")
            cursor = self.connection.cursor() 
            cursor.execute("SHOW TABLES")
            if not 'files_names' in cursor.fetchall():
                print("Table Apple_health.files_names not found\n Creating table ....")
                cursor.execute("CREATE TABLE files_names (type VARCHAR(100), creationDate DATE, value FLOAT(8), unit VARCHAR(100))")
                print("Table Apple_health.files_names Created")

            sql_query = "SELECT * FROM files_names"
            cursor.execute(sql_query)
            files_list = cursor.fetchall()

            return files_list

    def push_files_names(self, names_list):
            if self.connection.is_connected():
                print("Connection succeeded!!")
                cursor = self.connection.cursor()
                sql_query = "INSERT INTO files_names ('name') VALUES (%s)"     
                for name in names_list:
                    cursor.execute(sql_query,tuple(name))
                    self.connection.commit()
                return 1
            return 0



    def insertIntoMYSQL(self):
    
        if self.connection.is_connected():
            print("Connection succeeded!!")
            cursor = self.connection.cursor() 
            df = pd.read_csv(self.mysql_file)

            try:
                df.drop('Unnamed: 0', axis=1, inplace=True)
            except:
                pass
            
            cursor.execute("SHOW TABLES")
            if not 'health_data' in cursor.fetchall():
                print("Table Apple_health.heath_data not found\n Creating table ....")
                cursor.execute("CREATE TABLE health_data (type VARCHAR(100), creationDate DATE, value FLOAT(8), unit VARCHAR(100))")
                print("Table Apple_health.heath_data Created")
                
            sql_query, val = self.__pre_sql(df)
            print("Fetching Data .....")
            cursor.executemany(sql_query, val)
            self.connection.commit()
            print("Done.")
            return 1
        return 0
