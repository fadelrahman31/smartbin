import psycopg2
from flask import Flask

def getDataTPS(id):
    try:
        connection = psycopg2.connect(user = "ffgdwapi",
                                    password = "iAAuwugo4XEjormkKGmzW14-N6AWmDzD",
                                    host = "drona.db.elephantsql.com",
                                    port = "5432",
                                    database = "ffgdwapi")

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

def getDataDevice(id):
    try:
        connection = psycopg2.connect(user = "ffgdwapi",
                                    password = "iAAuwugo4XEjormkKGmzW14-N6AWmDzD",
                                    host = "drona.db.elephantsql.com",
                                    port = "5432",
                                    database = "ffgdwapi")

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


app = Flask(__name__)

#method index
@app.route('/')
def api_root():
    return 'SMART BIN: This means our server is running'

#method get retrieve data tps
@app.route('/data/tps/<id_tps>')
def api_data(id_tps):
    #return 'Membaca data dari Server ' + id_tps
    data = getDataTPS(id_tps)
    result = str(data)
    return result

#method get retrieve data device status
@app.route('/device/<id_device>')
def api_device(id_device):
    #return 'Membaca data dari Device ' + id_device
    data = getDataDevice(id_device)
    result = str(data)
    return result

if __name__ == '__main__':
    app.run()