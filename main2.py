from dateutil.relativedelta import relativedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
from bs4 import BeautifulSoup
from psycopg2 import sql
from db_config import get_db_connection


import requests
import re
import os
import pandas as pd

app = Flask(__name__)
CORS(app)

def extract_date_from_filename(filename):
    match = re.search(r'(\d{2})(\d{4})', filename)
    if match:
        month = int(match.group(1))
        year = int(match.group(2))
        date = datetime(year, month, 1)
        return date.strftime('%Y-%m-%d')
    else:
        raise ValueError("Невозможно извлечь дату из имени файла")

def download_file(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        return None, str(e)

    return response.content, None

def insert_into_db(filename, url, file_size):
    conn = get_db_connection()
    if conn is None:
        return "Connection to database failed"

    try:
        cur = conn.cursor()
        insert_query = sql.SQL(
            "INSERT INTO file_links (filename, url, size) VALUES (%s, %s, %s)"
        )
        cur.execute(insert_query, (filename, url, file_size))
        conn.commit()
        cur.close()
    except Exception as e:
        return str(e)
    finally:
        conn.close()
    return "Data inserted successfully"

def insert_deals_in_db(name):
    conn = get_db_connection()
    if conn is None:
        return "Connection to database failed"

    try:
        cur = conn.cursor()
        insert_query = sql.SQL(
            "INSERT INTO search_deal (name) VALUES (%s)"
        )
        cur.execute(insert_query, (name,))
        conn.commit()
        cur.close()
    except Exception as e:
        return str(e)
    finally:
        conn.close()
    return "Deal inserted successfully"

def insert_data_document_in_db(filename, region, seal, date, i):
    conn = get_db_connection()
    if conn is None:
        return "Connection to database failed"

    try:
        cur = conn.cursor()
        insert_query = sql.SQL(
            "INSERT INTO data (name, region, seal, date, search_deal_id) "
            "VALUES (%s, %s, %s, %s, %s)"
        )
        cur.execute(insert_query, (filename, region, seal, date, i))
        conn.commit()
        cur.close()
    except Exception as e:
        return str(e)
    finally:
        conn.close()
    return "Data document inserted successfully"

@app.route('/parseDataRosstat', methods=['GET'])
def parse_data_rosstat():
    url = "https://rosstat.gov.ru/uslugi"
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500

    if response.status_code != 200:
        return jsonify({"error": "Status code is not 200 OK"}), 500

    soup = BeautifulSoup(response.content, 'html.parser')
    re_pattern = re.compile(r'\d{6}')
    links = []

    for a_tag in soup.find_all('a', href=True):
        if 'mediabank' in a_tag['href']:
            matches = re_pattern.findall(a_tag['href'])
            if matches:
                full_url = "https://rosstat.gov.ru" + a_tag['href']
                links.append(full_url)

    processed_files = []
    for link in links:
        filename = os.path.basename(link)
        date = extract_date_from_filename(filename)

        if not os.path.exists(filename):
            file_content, error = download_file(link)
            if error:
                continue

            with open(filename, 'wb') as file:
                file.write(file_content)
            file_size = len(file_content)

            error = insert_into_db(filename, link, file_size)
            if error:
                continue

        if 'plat' in filename.lower():
            try:
                xls = pd.ExcelFile(filename)
                for sheet_name in xls.sheet_names[1:2]:
                    df = pd.read_excel(xls, sheet_name)
                    first_column = df.iloc[:, 0]
                    second_column = df.iloc[:, 1]

                    for region, seal in zip(first_column, second_column):
                        if pd.notna(region) and pd.notna(seal):
                            error = insert_data_document_in_db(filename, region, seal, date, 13)
                            if error:
                                continue
            except Exception as e:
                continue

        processed_files.append(filename)
    return jsonify({"processed_files": processed_files}), 200

@app.route('/insertDeal', methods=['POST'])
def insert_deal():
    data = request.json
    name = data.get('name')
    if not name:
        return jsonify({"error": "Name is required"}), 400
    result = insert_deals_in_db(name)
    return jsonify({"result": result}), 200

@app.route('/extractDate', methods=['POST'])
def extract_date():
    data = request.json
    filename = data.get('filename')
    if not filename:
        return jsonify({"error": "Filename is required"}), 400
    try:
        date = extract_date_from_filename(filename)
        return jsonify({"date": date}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

@app.route('/get/deal', methods=['GET'])
def get_deal():
    nameDeal = request.args.get('name')
    dateStart = request.args.get('dates')
    dateEnd = request.args.get('datee')
    region = request.args.get('region')

    print(nameDeal)
    print('dsadsa')

    if not nameDeal or not dateStart or not dateEnd or not region:
        return jsonify({"error": "Name, dates, and datee are required"}), 400

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = """
            SELECT data.id, data.name, data.region, data.seal, data.date,
                   search_deal.name, search_deal.percent
            FROM data
            JOIN search_deal ON data.search_deal_id = search_deal.id
            WHERE search_deal.name = %s
              AND data.date BETWEEN %s AND %s
              AND data.region = %s
        """
        cur.execute(query, (nameDeal, dateStart, dateEnd, region))
        rows = cur.fetchall()

        data = []
        for row in rows:
            data.append({
                "id": row[0],
                "name": row[1],
                "region": row[2],
                "seal": row[3],
                "date": row[4],
                "deal_name": row[5],
                "percent": row[6]
            })
        print(len(data))
        cur.close()
        conn.close()

        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/forecast-price', methods=['GET'])
def forecast_price():
    # Получение параметров из запроса
    deal_name = request.args.get('deal_name')
    region = request.args.get('region')
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')


    print(f"name: {deal_name}, dates: {date_start}, datee: {date_end}, region: {region}")

    # Проверка обязательных параметров
    if not deal_name or not region or not date_start or not date_end:
        return jsonify({"error": "Deal name, region, date_start, and date_end are required parameters"}), 400

    try:
        # Получение данных из базы данных
        conn = get_db_connection()
        cur = conn.cursor()

        query = """
            SELECT data.seal, data.date
            FROM data
            JOIN search_deal ON data.search_deal_id = search_deal.id
            WHERE search_deal.name = %s
              AND data.date BETWEEN %s AND %s
              AND data.region = %s
        """
        cur.execute(query, (deal_name, date_start, date_end, region))
        rows = cur.fetchall()

        # Проверка, что есть данные для обучения модели
        if not rows:
            return jsonify({"error": "No data found for the specified criteria"}), 404

        # Преобразование в DataFrame
        df = pd.DataFrame(rows, columns=['seal', 'date'])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

        alpha = 0.7  # Параметр сглаживания

        # Применяем экспоненциальное сглаживание ко всему столбцу 'seal'
        df['forecast'] = df['seal'].ewm(alpha=alpha, adjust=False).mean()

        # Получение последнего прогноза из данных
        last_forecast = df['forecast'].iloc[-1]

        # Прогноз на следующие 6 месяцев
        forecast_prices = []
        stData = datetime.strptime(date_end, '%Y-%m-%d')
        for i in range(1, 7):
            next_date = stData + relativedelta(months=i)
            print(next_date)
            # Рассчитываем прогноз для следующей даты, используя последний прогноз и параметр сглаживания alpha
            next_forecast = last_forecast * (1 - alpha) + alpha * df['forecast'].iloc[i]
            forecast_prices.append({"date": next_date.strftime('%Y-%m-%d'), "forecast_price": next_forecast})
            last_forecast = next_forecast

        return jsonify(forecast_prices), 200

    except Exception as e:
        return jsonify({"error": f"An error occurred: {e}"}), 500

@app.route('/')
def hello():
    return 'Hello, world!'

if __name__ == "__main__":
    app.run(debug=True, port=5000)
