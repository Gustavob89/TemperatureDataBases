import mariadb
import pymongo
from pymongo import MongoClient
import dns
import json

def ConnectToMariaDB(loginFileName):
    # Reads login data from txt file and connects to database;
    # returns conn variable if successful or 0 after fail

    loginInformationFile = open(loginFileName, "r")

    databaseIP = loginInformationFile.readline().rstrip()
    databasePort = loginInformationFile.readline().rstrip()
    databaseName = loginInformationFile.readline().rstrip()
    username0 = loginInformationFile.readline().rstrip()
    password0 = loginInformationFile.readline().rstrip()

    print("Attempting connection to database:", databaseName)
    print(" as:", username0)
    print(" at:", databaseIP + '/' + databasePort)

    try:
        conn = mariadb.connect(
            user=username0,
            password=password0,
            host=databaseIP,
            port=int(databasePort),
            database=databaseName
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        return 0

    return conn

def RetrieveRawData(conn, rowNumber):

    cursor = conn.cursor()

    statement = "Select * FROM sensor1data LIMIT " + str(rowNumber) + ",1"

    try:
        cursor.execute(statement)
    except mariadb.Error as e:
        print(f"Failed to retrieve data from rows in table sensoring1: {e}")
        input()
        exit()

    rawData = cursor.fetchone()

    cursor.close()
    return rawData

def ConvertDataFormat(rawData):

    dataList = list(rawData)

    # Convert year:
    temp = int(rawData[1]) + 1900
    dataList[1] = str(temp)

    # Convert month:
    temp = int(rawData[2]) + 1

    dataList[2] = temp

    for counter in range(2,7):
        if dataList[counter] < 10:
            temp = '0' + str(dataList[counter])
            dataList[counter] = temp
        else:
            temp = str(dataList[counter])
            dataList[counter] = temp

    return dataList

def buildJSON(sensorData):

    temperature = sensorData[0]
    dateTime = sensorData[1] + sensorData[2] + sensorData[3] + sensorData[4] + sensorData[5] + sensorData[6]

    pythonObject = {"datetime": dateTime, "temperature": temperature}
    jsonObject = pythonObject

    return jsonObject



if __name__ == '__main__':

    conn = ConnectToMariaDB("LoginData.txt")


    if conn == 0:
        print("Failed to connect to database")
        input()
        exit()
    else:
        print("Connected to MariaDB/Mysql database")

    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM sensor1data")
    except mariadb.Error as e:
        print(f"Failed to retrieve number of rows in table sensoring1: {e}")
        input()
        exit()

    result = cursor.fetchone()
    numberOfRows = result[0]

    cursor.close()


    #rawData = RetrieveRawData(conn,2500)

    #convertedData = ConvertDataFormat(rawData)

    #jsonObject = buildJSON(convertedData)


    # MongoDB Connection:
    MongoStringFile = open("MongoString.txt", "r")
    mongoString = MongoStringFile.readline()

    client = MongoClient(mongoString)
    db = client.sensoring_temperature
    collection = db.sensoring1



    for rowCounter in range(1, numberOfRows):
        rawData = RetrieveRawData(conn, rowCounter)
        convertedData = ConvertDataFormat(rawData)
        jsonObject = buildJSON(convertedData)

        resultArray = list(db.sensoring1.find({"datetime":jsonObject["datetime"]}))

        if len(resultArray) ==0:
            db.sensoring1.insert_one(jsonObject)

    print("Script complete")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
# https://pymongo.readthedocs.io/en/stable/tutorial.html