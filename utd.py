import mysql.connector
from mysql.connector import Error
import pandas as pd

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def connect_server(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password,
            database = db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

def drop_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database dropped successfully")
    except Error as err:
        print(f"Error: '{err}'")

def execute_table_creation_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

pw = "password" # put ur pw here
db = "UTDIsTheWorld"
firstconnection = create_server_connection("localhost", "root", pw)

drop_database_query = "DROP DATABASE UTDIsTheWorld"
drop_database(firstconnection, drop_database_query)

create_database_query = "CREATE DATABASE UTDIsTheWorld"
# create_database(connection, create_database_query)
create_database(firstconnection, create_database_query)

connection = connect_server("localhost", "root", pw, db)

create_person_table = """
CREATE TABLE Person (
    NetID VARCHAR(20) NOT NULL,
    DOB DATE NOT NULL,
    First_name VARCHAR(80) NOT NULL,
    Last_name VARCHAR(80) NOT NULL,
    Gender VARCHAR(20) NOT NULL,
    PRIMARY KEY (NetID)
  );
 """
execute_table_creation_query(connection, create_person_table)


create_organization_table = """
CREATE TABLE Organization (
    OrgName VARCHAR(120) NOT NULL,
    OrgWebsite VARCHAR(120),
    PRIMARY KEY (OrgName)
  );
 """
execute_table_creation_query(connection, create_organization_table)


create_office_table = """
CREATE TABLE Office (
    OName VARCHAR(120) NOT NULL,
    Website VARCHAR(120) NOT NULL,
    Address VARCHAR(120),
    Email VARCHAR(120) NOT NULL,
    Phone_number VARCHAR(20) NOT NULL,
    PRIMARY KEY (OName)
  );
 """
execute_table_creation_query(connection, create_office_table)


create_office_services_table = """
CREATE TABLE Office_Services (
    OName VARCHAR(120) NOT NULL,
    OServices SET('Student Affairs', 'Academics', 'Budget and Finance', 'Research and Development', 'Information', 'Other') NOT NULL,
    PRIMARY KEY (OName, OServices),
    FOREIGN KEY (OName) REFERENCES Office(OName)
  );
 """
execute_table_creation_query(connection, create_office_services_table)


create_school_table = """
CREATE TABLE School (
    SName VARCHAR(80) NOT NULL,
    SWebsite VARCHAR(120),
    PRIMARY KEY (SName)
  );
 """
execute_table_creation_query(connection, create_school_table)


create_school_locations_table = """
CREATE TABLE School_Locations (
    SName VARCHAR(80) NOT NULL,
    SLocation SET('North Campus', 'Northeast Campus', 'Northwest Campus', 'West Campus', 'East Campus', 'Southeast Campus', 'Southwest Campus', 'South Campus') NOT NULL,
    PRIMARY KEY (SName, SLocation),
    FOREIGN KEY (SName) REFERENCES School(SName)
  );
 """
execute_table_creation_query(connection, create_school_locations_table)


create_administrator_table = """
CREATE TABLE Administrator (
    NetID VARCHAR(20) NOT NULL,
    ATitle VARCHAR(180) NOT NULL,
    OfficeName VARCHAR(120) NOT NULL,
    PRIMARY KEY (NetID),
    FOREIGN KEY (NetID) REFERENCES Person(NetID),
    FOREIGN KEY (OfficeName) REFERENCES Office(OName)
  );
 """
execute_table_creation_query(connection, create_administrator_table)


create_advisor_table = """
CREATE TABLE Advisor (
    NetID VARCHAR(20) NOT NULL,
    AdvEmail VARCHAR(120) NOT NULL,
    SchoolName VARCHAR(80) NOT NULL,
    PRIMARY KEY (NetID),
    FOREIGN KEY (NetID) REFERENCES Administrator(NetID),
    FOREIGN KEY (SchoolName) REFERENCES School(SName)
  );
 """
execute_table_creation_query(connection, create_advisor_table)

#Name_range VARCHAR(20) NOT NULL,

create_student_table = """
CREATE TABLE Student (
    NetID VARCHAR(20) NOT NULL,
    Classification VARCHAR(20) NOT NULL,
    StudentSchoolName VARCHAR(80) NOT NULL,
    PRIMARY KEY (NetID),
    FOREIGN KEY (NetID) REFERENCES Person(NetID),
    FOREIGN KEY (StudentSchoolName) REFERENCES School(SName)
  );
 """
execute_table_creation_query(connection, create_student_table)

#StudentNameRange VARCHAR(20) NOT NULL,
#FOREIGN KEY (StudentNameRange) REFERENCES Advisor(Name_range)

create_professor_table = """
CREATE TABLE Professor (
    NetID VARCHAR(20) NOT NULL,
    PTitle VARCHAR(180) NOT NULL,
    PRIMARY KEY (NetID),
    FOREIGN KEY (NetID) REFERENCES Person(NetID)
  );
 """
execute_table_creation_query(connection, create_professor_table)


create_course_table = """
CREATE TABLE Course (
    C_number INT UNIQUE,
    School_name VARCHAR(80) NOT NULL,
    Credit_hours INT,
    CName VARCHAR(60) NOT NULL,
    PRIMARY KEY (C_number),
    FOREIGN KEY (School_name) REFERENCES School(SName)
  );
 """
execute_table_creation_query(connection, create_course_table)


create_section_table = """
CREATE TABLE Section (
    Section_number VARCHAR(6) NOT NULL,
    Course_number INT,
    ProfNetID VARCHAR(10) NOT NULL,
    STime VARCHAR(20) NOT NULL,
    Days VARCHAR(10) NOT NULL,
    SRoom VARCHAR(10),
    PRIMARY KEY (Section_number, Course_number),
    FOREIGN KEY (Course_number) REFERENCES Course(C_number),
    FOREIGN KEY (ProfNetID) REFERENCES Professor(NetID)
  );
 """
execute_table_creation_query(connection, create_section_table)


create_event_table = """
CREATE TABLE Event (
    OrgName VARCHAR(120) NOT NULL,
    StudentNetID VARCHAR(20) NOT NULL,
    EName VARCHAR(120) NOT NULL,
    ETime VARCHAR(40) NOT NULL,
    Day DATE NOT NULL,
    ERoom VARCHAR(20) NOT NULL,
    PRIMARY KEY (EName, OrgName),
    FOREIGN KEY (OrgName) REFERENCES Organization(OrgName),
    FOREIGN KEY (StudentNetID) REFERENCES Student(NetID)
  );
 """
execute_table_creation_query(connection, create_event_table)


create_enrolls_table = """
CREATE TABLE Enrolls (
    StudentNetID VARCHAR(20) NOT NULL,
    SectionNumber VARCHAR(6) NOT NULL,
    PRIMARY KEY (StudentNetID, SectionNumber),
    FOREIGN KEY (StudentNetID) REFERENCES Student(NetID),
    FOREIGN KEY (SectionNumber) REFERENCES Section(Section_number)
  );
 """
execute_table_creation_query(connection, create_enrolls_table)


create_joins_table = """
CREATE TABLE Joins (
    StudentNetID VARCHAR(20) NOT NULL,
    OrgName VARCHAR(120) NOT NULL,
    PRIMARY KEY (StudentNetID, OrgName),
    FOREIGN KEY (StudentNetID) REFERENCES Student(NetID),
    FOREIGN KEY (OrgName) REFERENCES Organization(OrgName)
  );
 """
execute_table_creation_query(connection, create_joins_table)


create_sponsors_table = """
CREATE TABLE Sponsors (
    ProfessorNetID VARCHAR(20) NOT NULL,
    OrgName VARCHAR(120) NOT NULL,
    PRIMARY KEY (ProfessorNetID, OrgName),
    FOREIGN KEY (ProfessorNetID) REFERENCES Professor(NetID),
    FOREIGN KEY (OrgName) REFERENCES Organization(OrgName)
  );
 """
execute_table_creation_query(connection, create_sponsors_table)


def execute_insertion_query(db_connection, table_name, variable_names_list):
    cursor = db_connection.cursor()
    data_frame = pd.read_csv(f"{table_name.lower()}_data.csv")
    variable_names_sql = ", ".join(variable_names_list)
    format_specifiers_sql = ", ".join(["%s"] * len(variable_names_list))
    sql = f"INSERT INTO {table_name}({variable_names_sql}) VALUES ({format_specifiers_sql})"
    for item_index, item in data_frame.iterrows():
        val = list()
        for variable_name in variable_names_list:
            item_str = str(item[variable_name])
            if item_str.startswith("{") and item_str.endswith("}"):
                item_str = item_str.replace("{'", "")
                item_str = item_str.replace("'}", "")
                item_str = item_str.replace("', '", ",")
            val.append(item_str)
        cursor.execute(sql, val)
    db_connection.commit()


table_header_data = {
    "Person": ["First_name", "Last_name", "DOB", "Gender", "NetID"],
    "Organization": ["OrgName", "OrgWebsite"],
    "Office": ["OName", "Website", "Address", "Email", "Phone_number"],
    "Office_Services": ["OName", "OServices"],
    "School": ["SName", "SWebsite"],
    "School_Locations": ["SName", "SLocation"],
    "Administrator": ["NetID", "OfficeName", "ATitle"],
    "Advisor": ["NetID", "AdvEmail", "SchoolName"],
    "Student": ["NetID", "Classification", "StudentSchoolName"],
    "Professor": ["NetID", "PTitle"],
    "Course": ["C_number", "School_name", "Credit_hours", "CName"],
    "Section": ["Section_number", "Course_number", "ProfNetID", "STime", "Days", "SRoom"],
    "Event": ["EName", "OrgName", "ERoom", "Day", "ETime", "StudentNetID"],
    "Enrolls": ["StudentNetID", "SectionNumber"],
    "Joins": ["StudentNetID", "OrgName"],
    "Sponsors": ["ProfessorNetID", "OrgName"]
}


for name, variable_names in table_header_data.items():
    execute_insertion_query(connection, name, variable_names)

