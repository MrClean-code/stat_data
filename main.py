import requests
from bs4 import BeautifulSoup
import re
import os
import pandas as pd

def download_file(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Ошибка при загрузке файла: {e}")
        return None

    return response.content

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

        print(f"Скачан файл: {link} (размер: {len(file_content)} байт)")

        # Сохранение файла на диск
        filename = os.path.basename(link)
        try:
            with open(filename, 'wb') as file:
                file.write(file_content)
        except IOError as e:
            print(f"Ошибка при сохранении файла {filename}: {e}")
            continue

        # Чтение и вывод содержимого файла с использованием pandas
        try:
            df = pd.read_excel(filename)
            print(f"Содержимое файла {filename}:")
            print(df)
        except Exception as e:
            print(f"Ошибка при чтении файла {filename} с использованием pandas: {e}")

if __name__ == "__main__":
    main()
