import psycopg2
from flask import Flask, request, json
from dotenv import load_dotenv
import os

load_dotenv()

def getDataTPS(id):
    try:
        connection = psycopg2.connect(user = os.getenv("DB_USER"),
                                    password = os.getenv("DB_PASS"),
                                    host = os.getenv("DB_HOST"),
                                    port = os.getenv("DB_PORT"),
                                    database = os.getenv("DATABASE")
                                    )

        cursor = connection.cursor()

        #run some SQL query
        get_query = """select * from datatps where id_tps = %s"""
        cursor.execute(get_query, (id, ))
        results = cursor.fetchall()
        #print("result ", results)
        return results

    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

def getAllDataTPS():
    try:
        connection = psycopg2.connect(user = os.getenv("DB_USER"),
                                    password = os.getenv("DB_PASS"),
                                    host = os.getenv("DB_HOST"),
                                    port = os.getenv("DB_PORT"),
                                    database = os.getenv("DATABASE")
                                    )

        cursor = connection.cursor()

        #run some SQL query
        get_query = """select * from datatps"""
        cursor.execute(get_query)
        results = cursor.fetchall()
        #print("result ", results)
        return results

    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

def getDataDevice(id):
    try:
        connection = psycopg2.connect(user = os.getenv("DB_USER"),
                                    password = os.getenv("DB_PASS"),
                                    host = os.getenv("DB_HOST"),
                                    port = os.getenv("DB_PORT"),
                                    database = os.getenv("DATABASE")
                                    )

        cursor = connection.cursor()

        #run some SQL query
        get_query = """select * from device where id_device = %s"""
        cursor.execute(get_query, (id, ))
        results = cursor.fetchall()
        #print("result ", results)
        return results

    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

def postDataTPS(id_tps, waktu, humidity, temp, latitude, longitude, city):
    try:
        connection = psycopg2.connect(user = os.getenv("DB_USER"),
                                    password = os.getenv("DB_PASS"),
                                    host = os.getenv("DB_HOST"),
                                    port = os.getenv("DB_PORT"),
                                    database = os.getenv("DATABASE")
                                    )

        cursor = connection.cursor()

        #run some SQL query
        post_query = """ INSERT INTO datatps (id_tps, waktu, humidity, temp, latitude, longitude, city) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
        #data_insert = ('tps01', '9-April-2020', 750, 360)
        data_insert = (id_tps, waktu, float(humidity), float(temp), float(latitude), float(longitude), city)
        cursor.execute(post_query, data_insert)

        connection.commit()
        #count = cursor.rowcount()
        print("Data berhasil dimasukkan kedalam tabel datatps")

    except (Exception, psycopg2.Error) as error :
        print ("gagal untuk insert data ke tabel", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")



app = Flask(__name__)

#method index
@app.route('/')
def api_root():
    return 'SMART BIN: This means our server is running'

#method get retrieve data tps by id
@app.route('/data/tps/<id_tps>')
def api_data(id_tps):
    data = getDataTPS(id_tps)
    result = str(data)
    return result

#method get retrieve all data tps
@app.route('/data/tps')
def api_all_data():
    data = getAllDataTPS()
    result = str(data)
    return result

#method get retrieve data device status
@app.route('/device/<id_device>')
def api_device(id_device):
    data = getDataDevice(id_device)
    result = str(data)
    return result

#method post transmit data tps
@app.route('/data/tps/<id_tps>', methods = ['POST'])
def api_transmit_data(id_tps):
    if request.headers['Content-Type'] == 'application/json':
        #respond here
        #a = json.dumps(request.json)
        a = (request.json)
        #print(type(a))
        id_tps      = a['id_tps']
        waktu       = a['waktu']
        humidity    = a['humidity']
        temp        = a['temp']
        latitude    = a['latitude']
        longitude   = a['longitude']
        city        = a['city']
        #execute the Query with Fuction
        postDataTPS(id_tps, waktu, humidity, temp, latitude, longitude, city)
        return "Berhasil mnyimpan data dari TPS " + id_tps +" !"
        #untuk notes, a = {'id_tps': 'tps01', 'waktu': '9-april-2020 13.45WIB', 'humidity': '33.0', 'temp': '35.5', 'latitude': '126.0','longitude': '97.0', 'city': 'Bandung'}
    else:
        return "415:: Unsupported Media Type!"

if __name__ == '__main__':
    app.run()