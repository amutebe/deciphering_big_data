import mysql.connector
from mysql.connector import Error

'''
mydb = mysql.connector.connect(
    host = "localhost",
    user = "sa",
    password = "JosHa1234!",
    port = "3308"
)
 
# Printing the connection object 
print(mydb)'''
try:
    connection = mysql.connector.connect(host='localhost',database='realestate',user='sa',password='JosHa1234!', port='3308')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
except Error as e:
       print("Error while connecting to MySQL", e)
finally:
    
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

