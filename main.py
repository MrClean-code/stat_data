import requests
from bs4 import BeautifulSoup
import re
import os
from psycopg2 import sql
from db_config import get_db_connection

def download_file(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Ошибка при загрузке файла: {e}")
        return None

    return response.content

def insert_into_db(filename, url, file_size):
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        insert_query = sql.SQL(
            "INSERT INTO file_links (filename, url, size) VALUES (%s, %s, %s)"
        )
        cur.execute(insert_query, (filename, url, file_size))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Ошибка при вставке данных в базу данных: {e}")
    finally:
        conn.close()

def main():
    url = "https://rosstat.gov.ru/uslugi"

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Ошибка при получении страницы: {e}")
        return

    if response.status_code != 200:
        print("Статус-код не является 200 OK")
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    re_pattern = re.compile(r'\d{6}')
    links = []

    for a_tag in soup.find_all('a', href=True):
        if 'mediabank' in a_tag['href']:
            matches = re_pattern.findall(a_tag['href'])
            if matches:
                full_url = "https://rosstat.gov.ru" + a_tag['href']
                links.append(full_url)

    for link in links:
        file_content = download_file(link)
        if file_content is None:
            print(f"Ошибка при скачивании файла {link}")
            continue

        file_size = len(file_content)
        print(f"Скачан файл: {link} (размер: {file_size} байт)")

        # Сохранение файла на диск
        # filename = os.path.basename(link)
        # try:
        #     with open(filename, 'wb') as file:
        #         file.write(file_content)
        # except IOError as e:
        #     print(f"Ошибка при сохранении файла {filename}: {e}")
        #     continue

        # Вставка данных в базу данных
        # insert_into_db(os.path.basename(link), link, file_size)

        # Чтение и вывод содержимого файла с использованием pandas
        # try:
        #     df = pd.read_excel(filename)
        #     print(f"Содержимое файла {filename}:")
        #     print(df)
        # except Exception as e:
        #     print(f"Ошибка при чтении файла {filename} с использованием pandas: {e}")
if __name__ == "__main__":
    main()
