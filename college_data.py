import os
from db_connector import DBConnector
from common_utils import load_json
from prettytable import PrettyTable


class CollegeDataProcessor:
    def __init__(self) -> None:
        db_config = load_json("db_connection.json")["config"]
        self._db_conn = DBConnector(db_config["db_user"], db_config["db_pass"],  db_config["dsn"], db_config["port"])
        print("[INFO] Initiallizing databbase....")
        self._initiallize()
        print("[INFO] Database initialization completed.")


    def _initiallize(self):
        db_data = load_json("db_data.json")
        table_data = db_data["table_data"]
        # table_names = table_data.keys()
        drop_constraints = db_data["drop_constraints"]

        for constraint in drop_constraints:
            self._db_conn.execute_proc(constraint)

        for table_info in table_data:
            table = list(table_info.keys())[0]
            data = table_info[table]

            self._db_conn.execute_proc(f"""
                                    begin
                                        execute immediate 'drop table {table} cascade constraints';
                                        exception when others then if sqlcode <> -942 then raise; end if;
                                    end;""")
            self._db_conn.create_table(data["schema"])
            self._db_conn.batch_insert(table, data["insert_columns"], data["data"])


    def _get_table_columns(self, table_name):
        columns = self._db_conn.select("USER_TAB_COLUMNS", ["column_name"], f"table_name = '{table_name.upper()}'")
        columns_to_show = []
        for column in columns:
            columns_to_show.append(column[0])

        return columns_to_show
    

    def show_data(self, table_name, condition=None):
       #"SELECT column_name FROM USER_TAB_COLUMNS WHERE table_name = 'MYTABLE'"
        columns_to_show = self._get_table_columns(table_name)
        data_table = PrettyTable(columns_to_show)

        rows = self._db_conn.select(table_name, condition=condition)
        for row in rows:
            data_table.add_row(row)

        print(data_table.get_formatted_string())
    
    def show_single_data(self, table_name, id):
        self.show_data(table_name, "id="+str(id))

    
    def _get_data_dict(self, columns, values):
        data_dict = {}
        for cnt in range(1, len(columns)):
            data_dict[columns[cnt]] = values[cnt]

        return data_dict
    

    def update_data(self, table_name, id):
        columns_to_update = self._get_table_columns(table_name)
        rows = self._db_conn.select(table_name, condition="id="+str(id))
        if len(rows) > 0:
            table_data = self._get_data_dict(columns_to_update, rows[0])
            updated_data = {}
            print("Updated values or keep the current: ")
            for column, value in table_data.items():
                new_value = input(f"Enter value for '{column}' (Current value: {value}): ")
                if new_value:
                    updated_data[column] = new_value
                else:
                    updated_data[column] = value

            print(updated_data)
            self._db_conn.update(table_name, updated_data, id)
        else:
            print(f"[WARN] Data with {id} is not present in '{table_name}'")

    
    def delete_data(self, table_name, id):
        self._db_conn.delete(table_name, id)

    
    def show_joined_data(self, table_name1, table_name2, table_name3):
        query = f"""select {table_name1}.*,{table_name2}.* from {table_name1}, {table_name2}, {table_name3} 
                    where {table_name3}.{table_name1} = {table_name1}.id and {table_name3}.{table_name2} = {table_name2}.id"""
        
        rows = self._db_conn.execute_proc(query)
        print([row[0] for row in rows.description])

    
    def show_prof_branch(self):
        rows = self._db_conn.execute_proc(" select b.name as branch, p.name as professor, p.surname as surname from branch b, professor p, prof_branch pb where pb.professor = p.id and pb.branch = b.id")
        data_table = PrettyTable(["Branch", "Name", "Surname"])

        for row in rows:
            data_table.add_row(row)

        print(data_table.get_formatted_string())


    def show_stud_branch(self):
        rows = self._db_conn.execute_proc(" select s.name as name, s.surname as surname, b.name as branch from student s, branch b, stud_branch sb where sb.student = s.id and sb.branch = b.id")
        data_table = PrettyTable(["Name", "Surname", "Branch"])

        for row in rows:
            data_table.add_row(row)

        print(data_table.get_formatted_string())


    def show_stud_sub(self):
        rows = self._db_conn.execute_proc(" select s.name as name, s.surname as surname, sub.name as subject from student s, subject sub, stud_sub ss where ss.student = s.id and ss.subject = sub.id")
        data_table = PrettyTable(["Name", "Surname", "Subject"])

        for row in rows:
            data_table.add_row(row)

        print(data_table.get_formatted_string())

    
    def show_prof_sub(self):
        rows = self._db_conn.execute_proc(" select p.name as name, p.surname as surname, sub.name as subject from professor p, subject sub, prof_sub ps where ps.professor = p.id and ps.subject = sub.id")
        data_table = PrettyTable(["Name", "Surname", "Subject"])

        for row in rows:
            data_table.add_row(row)

        print(data_table.get_formatted_string())




def show_submenu(table_name):
    while True:
        print(f"\n\n===================={table_name}'s Menu=================")
        print(f"\t\t1. Show all {table_name}s")
        print(f"\t\t2. Show Single {table_name}")
        print(f"\t\t3. Update {table_name}")
        print(f"\t\t4. Delete {table_name}")
        print("\t\t5. Back")
        stud_choice = int(input("Enter your choice: "))
        match stud_choice:
            case 1:
                processor.show_data(table_name)
            case 2:
                id = int(input(f"Enter {table_name} id: "))
                processor.show_single_data(table_name, id)
            case 3:
                id = int(input(f"Enter {table_name} id: "))
                processor.update_data(table_name, id)
            case 4:
                id = int(input(f"Enter {table_name} id: "))
                processor.delete_data(table_name, id)
            case 5:
                break


processor = CollegeDataProcessor()

while True:
    os.system("clear")
    print("\n\n====================<Collage Name>=================")
    print("\t\t1. Students")
    print("\t\t2. Branches")
    print("\t\t3. Professors")
    print("\t\t4. Subjects")
    print("\t\t5. Students and their branches")
    print("\t\t6. Students and their subjects")
    print("\t\t7. Professor and their branch")
    print("\t\t8. Professor and their subjects")
    print("\t\t9. Exit")

    choice = int(input("Enter your choice: "))
    match choice:
        case 1:
            show_submenu("student")
        case 2:
            show_submenu("branch")
        case 3:
            show_submenu("professor")
        case 4:
            show_submenu("subject")
        case 5:
            processor.show_stud_branch()
            input("Press enter to continue....")
        case 6:
            processor.show_stud_sub()
            input("Press enter to continue....")
        case 7:
            processor.show_prof_branch()
            input("Press enter to continue....")
        case 8:
            processor.show_prof_sub()
            input("Press enter to continue....")
        case 9:
            break



                

