from flask import Flask, make_response
import psycopg2
from psycopg2 import OperationalError
import pytz
from datetime import datetime, time
import csv
import uuid
import io

app = Flask(__name__)
db_host = 'localhost'
db_port = '5432'
db_name = 'store_data'
db_user = 'postgres'
db_password = '1998sam'
current_time = '2023-01-25 18:13:22.47922'

def get_db_connection():
    '''
    Connecting to the database.
    '''
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
    except OperationalError as e:
        print("Error connecting to database {str(e)}")
    return conn

def add_dow_status():
    '''
    Adds Day of Week to timestamp_utc for easier calculation.
    '''
    try:
        conn = get_db_connection()
        if conn is None:
            return 'Error connecting to the database'
        cursor = conn.cursor()
        cursor.execute("""
                SELECT column_name
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = 'store_status'
                AND column_name = 'day_of_week';
            """)
        result = cursor.fetchone()
        if result:
            return 'Column exists'        
        cursor.execute("""
                ALTER TABLE store_status
                    ADD COLUMN day_of_week INTEGER;
        """)
        cursor.execute("""
                UPDATE store_status
                    SET day_of_week = EXTRACT(DOW FROM timestamp_utc)::INTEGER;
        """)
        conn.commit()
        conn.close()
        return 'Column exists'
    except Exception as e:
        return f'An error occurred: {str(e)}'

def convert_time_utc(time, timezone):
    '''
    Converting timestamps in local timezone to UTC.
    '''
    local_timezone = pytz.timezone(timezone)
    time_str = time.strftime('%H:%M:%S.%f')
    utc_timezone = pytz.timezone('UTC')
    datetime_obj = datetime.datetime.strptime(time_str, '%H:%M:%S.%f')
    utc_time = local_timezone.localize(datetime_obj).astimezone(utc_timezone)
    return utc_time

def is_within_time_range(timestamp, time_1, time_2):
    '''
    Helper function to determine whether the timestamp lies between the range of opening and closing times for that day.
    '''
    if time_1==time(0,0,0) and time_2==time(23,59,59):
        return True
    timestamp_time = timestamp.time()
    time_1_obj = time.fromisoformat(str(time_1))
    time_2_obj = time.fromisoformat(str(time_2))
    if time_1_obj <= timestamp_time <= time_2_obj:
        return True
    else:
        return False
    
def generate_report_id():
    '''
    Generates a unique report_id
    '''
    report_id = str(uuid.uuid4())[:19]
    return report_id

def create_report(store_id, timezone):
    '''
    Calculates the required parameters and creates a report for them.
    '''
    res = add_dow_status()
    if(res!='Column exists'):
        return "Error occured while creating day_of_week in store_status"
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        start_time = [0]*7
        end_time = [0]*7
        store_id = str(store_id)
        query = "SELECT start_time_local,end_time_local, day_of_week FROM store_timing WHERE store_id = %s;"
        cursor.execute(query,(store_id,))
        result = cursor.fetchall()
        for i in range(len(result)):
            start_time[result[i][2]] = result[i][0]
            end_time[result[i][2]] = result[i][1]
        for i in range(7):
            if(start_time[i]==0):
                start_time[i] = time(0,0,0)
                end_time[i] = time(23,59,59)

        # Calculating Uptime and Downtime for last hour
        query = """SELECT timestamp_utc, day_of_week, status
                FROM store_status
                WHERE store_id = %s AND timestamp_utc >= timestamp %s - interval '1 hour'
                AND timestamp_utc < timestamp %s;"""
        cursor.execute(query,(store_id,current_time,current_time))
        result = cursor.fetchall()
        total = 0
        count = 0
        uptime_last_hour = 0
        downtime_last_hour = 60
        for timestamp in result:
            timestamp_local = pytz.timezone('UTC').localize(timestamp[0]).astimezone(pytz.timezone(timezone))
            if(is_within_time_range(timestamp_local,start_time[timestamp[1]],end_time[timestamp[1]])):
                total += 1
                if (timestamp[2]=='active'):
                    count += 1
                    curr = datetime.strptime(current_time,'%Y-%m-%d %H:%M:%S.%f')
                    uptime_last_hour = (curr - timestamp[0]).total_seconds()/60 + 30 
        if(count==total):
            downtime_last_hour = 0
        else:
            downtime_last_hour = (total-count)*20

        # Calculating Uptime and Downtime for last day
        query = """SELECT timestamp_utc, day_of_week, status
                FROM store_status
                WHERE store_id = %s AND timestamp_utc >= timestamp %s - interval '1 day'
                AND timestamp_utc < timestamp %s;"""
        cursor.execute(query,(store_id,current_time,current_time))
        result = cursor.fetchall()
        count = 0
        for timestamp in result:
            timestamp_local = pytz.timezone('UTC').localize(timestamp[0]).astimezone(pytz.timezone(timezone))
            if(is_within_time_range(timestamp_local,start_time[timestamp[1]],end_time[timestamp[1]]) and timestamp[2]=='inactive'):
                count += 1
        if(len(result)!=0 and count==0):
            datetime1 = datetime.combine(datetime.today(), start_time[timestamp[1]])
            datetime2 = datetime.combine(datetime.today(), end_time[timestamp[1]])
            uptime_last_day = (datetime2 - datetime1).total_seconds() / 3600
            downtime_last_day = 0
        elif(len(result)!=0):
            datetime1 = datetime.combine(datetime.today(), start_time[timestamp[1]])
            datetime2 = datetime.combine(datetime.today(), end_time[timestamp[1]])
            uptime_last_day = ((datetime2 - datetime1).total_seconds() / 3600) - (count/3)
            downtime_last_day = count/3
        else:
            current_datetime = datetime.strptime(current_time,'%Y-%m-%d %H:%M:%S.%f')
            dow = current_datetime.weekday()
            datetime1 = datetime.combine(datetime.today(), start_time[dow])
            datetime2 = datetime.combine(datetime.today(), end_time[dow])
            uptime_last_day = 0
            downtime_last_day = (datetime2 - datetime1).total_seconds() / 3600

        # Calculating Uptime and Downtime for last week
        query = """SELECT timestamp_utc, day_of_week, status
                FROM store_status
                WHERE store_id = %s AND timestamp_utc >= timestamp %s - interval '1 week'
                AND timestamp_utc < timestamp %s;"""
        cursor.execute(query,(store_id,current_time,current_time))
        result = cursor.fetchall()
        count = 0
        total_time = 0
        for i in range(7):
            datetime1 = datetime.combine(datetime.today(), start_time[i])
            datetime2 = datetime.combine(datetime.today(), end_time[i])
            total_time += (datetime2 - datetime1).total_seconds() / 3600
        for timestamp in result:
            timestamp_local = pytz.timezone('UTC').localize(timestamp[0]).astimezone(pytz.timezone(timezone))
            if(is_within_time_range(timestamp_local,start_time[timestamp[1]],end_time[timestamp[1]]) and timestamp[2]=='inactive'):
                count += 1
        if(count==0):
            uptime_last_week = total_time
            downtime_last_week = 0
        else:
            uptime_last_week = total_time - (count/3)
            downtime_last_week = count/3

        # Creating a csv containing the report for the store
        filename = f'reports/{store_id}.csv'
        data = []
        data.append(str(store_id))
        data.append(str(uptime_last_hour))
        data.append(str(uptime_last_day))
        data.append(str(uptime_last_week))
        data.append(str(downtime_last_hour))
        data.append(str(downtime_last_day))
        data.append(str(downtime_last_week))
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(data)

        # Adding the report to the database
        report_id = generate_report_id()
        report_csv_path = f'C:/Users/Maithri/Desktop/store_management/{filename}'
        query = """INSERT INTO store_report (report_id, store_id, report_csv_path) VALUES (%s,%s,%s)
        """
        cursor.execute(query,(report_id,store_id,report_csv_path))
        conn.commit()
        conn.close()
        return report_id
    except Exception as e:
        return f'An error occurred: {str(e)}'

@app.route('/trigger_report')
def trigger_report():
    '''
    Triggers the report generation
    '''
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM store_timezone LIMIT 3')
    result = cursor.fetchall()
    res = []
    for store in result:
        st = create_report(store[0],store[1])
        res.append(st)
    conn.close()
    return str(res[2])

@app.route('/get_report/<report_id>', methods=['GET'])
def get_report(report_id):
    '''
    Fetches the report
    '''
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """SELECT report_id, store_id, report_csv_path FROM store_report WHERE report_id = %s
        """
        cursor.execute(query,(report_id,))
        result = cursor.fetchone()
        conn.close()
        if (result):
            csv_file_path = result[2]
            csv_data = io.BytesIO()
            with open(csv_file_path, 'rb') as f:
                csv_data.write(f.read())
            response = make_response(csv_data.getvalue())
            response.headers.set('Content-Type', 'text/csv')
            response.headers.set('Content-Disposition', 'attachment')
            response.data += b'\nComplete.'
            return response
        else:
            return "Running"
    except Exception as e:
        return f'An error occurred while fetching the report: {str(e)}'



if __name__ == '__main__':
    app.run(debug=True)