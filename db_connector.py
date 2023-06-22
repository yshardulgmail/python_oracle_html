import oracledb

class DBConnector:
    def __init__(self, username, password, dsn="localhost/xepdb1", port=1521) -> None:
        try:
            self.conn = oracledb.connect(
                            user=username,
                            password=password,
                            dsn=dsn,
                            port=port)
            self.cursor = self.conn.cursor()
            self.conn.autocommit = True
            print("[INFO] Database connection was successful!!")
        except:
            raise Exception(f"[ERROR] Cannot connect to '{dsn}' with user as '{username}'.")
        
        
    def _execute_query(self, query, values=None, batch=False):
        ret_value = None
        try:
            if values:
                if not batch:
                    ret_value = self.cursor.execute(query, values)
                else:
                    ret_value = self.cursor.executemany(query, values)
            else:
                ret_value = self.cursor.execute(query)
            # self.conn.commit()
        except Exception as e:
            print("[ERROR] Got error while executing query - ")
            print("[ERROR] Query:", query)
            print("[ERROR] Error:", str(e))
            # exit(0)
        return ret_value
        
            
    def insert(self, table_name, columns, *args):
        # insert_args = ",".join([f":var{cnt}" for cnt in range(0, len(args))])
        insert_args = []
        for cnt in range(0, len(args)):
            insert_args.append(f":var{cnt}")

        columns_str = ",".join(columns)
        insert_args_str = ",".join(insert_args)
        query = f"insert into {table_name} ({columns_str}) values({insert_args_str})"
        if self._execute_query(query, list(args)):
            print("[INFO] Data inserted!!")


    def batch_insert(self, table_name, columns, values):
        insert_args = []
        for cnt in range(0, len(values[0])):
            insert_args.append(f":{cnt}")

        columns_str = ",".join(columns)
        insert_args_str = ",".join(insert_args)
        insert_values = []
        for value in values:
            if isinstance(value, list):
                insert_values.append(tuple(value))
            else:
                insert_values.append(value)

        query = f"insert into {table_name} ({columns_str}) values({insert_args_str})"
        if self._execute_query(query, insert_values, True):
            print("[INFO] Data inserted in batch!!")


    def select(self, table_name, columns="*", condition=None):
        columns_str = "*"
        if columns != "*":
            columns_str = ",".join(columns)

        query = f"select {columns_str} from {table_name}"
        if condition:
            query += " where " + condition

        ret_cursor = self._execute_query(query)
        rows = []
        if ret_cursor:
            rows = ret_cursor.fetchall()
            print("[INFO] No of selected records: ", len(rows))
        else:
            print("[INFO] 0 records selected.")
        
        return rows
    

    def update(self, table_name, update_data, id):
        update_columns = []
        for column, value in update_data.items():
            if isinstance(value, str):
                update_columns.append(f"{column}='{value}'")
            else:
                update_columns.append(f"{column}={value}")

        query = f"update {table_name} set {','.join(update_columns)} where id={id}"
        if self._execute_query(query):
            print("[INFO] Table record has been updated!!")


    def delete(self, table_name, id):
        query = f"delete from {table_name} where id={id}"
        if self._execute_query(query):
            print("[INFO] Record has been deleted!!")


    def create_table(self, create_str):
        if self._execute_query(create_str):
            print("[INFO] Table created!!")


    def execute_proc(self, proc_str):
        return self._execute_query(proc_str)
            

        