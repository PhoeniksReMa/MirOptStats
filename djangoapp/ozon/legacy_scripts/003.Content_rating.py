import os
import requests
import gspread
from google.oauth2.service_account import Credentials

# Получаем абсолютный путь к файлу credentials.json
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(SCRIPT_DIR, "credentials.json")

# Настройки Google Sheets API
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def read_api_file(file_path):
    """Чтение данных из файла API.txt с обработкой разных кодировок"""
    encodings_to_try = ['utf-8', 'cp1251', 'utf-16']

    for encoding in encodings_to_try:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                lines = [line.strip() for line in file.readlines() if line.strip()]

                if len(lines) < 4:
                    raise ValueError(f"Файл API.txt должен содержать 4 строки. Найдено: {len(lines)}")

                return lines[0], lines[1], lines[2], lines[3]

        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Ошибка при чтении файла с кодировкой {encoding}: {str(e)}")
            continue

    raise ValueError(f"Не удалось прочитать файл {file_path} с кодировками: {', '.join(encodings_to_try)}")

# Читаем данные из файла
API_FILE = os.path.join(SCRIPT_DIR, "API.txt")
CLIENT_ID, API_KEY, SPREADSHEET_ID, SHEET_NAME = read_api_file(API_FILE)

def get_product_ratings_batch(sku_list):
    url = "https://api-seller.ozon.ru/v1/product/rating-by-sku"
    headers = {
        "Client-Id": CLIENT_ID,
        "Api-Key": API_KEY,
        "Content-Type": "application/json"
    }
    payload = {"skus": sku_list}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[ОШИБКА] Запрос не удался для группы SKU: {str(e)}")
        return None

def main():
    try:
        print("[ИНФО] Проверка файла credentials...")
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            raise FileNotFoundError(
                f"Файл учетных данных не найден: {SERVICE_ACCOUNT_FILE}\n"
                f"Текущая рабочая директория: {os.getcwd()}"
            )

        print("[ИНФО] Авторизация в Google Sheets...")
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)

        print("[ИНФО] Открытие таблицы...")
        try:
            sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
        except gspread.exceptions.APIError as e:
            raise Exception("Ошибка доступа к таблице. Проверьте SPREADSHEET_ID и SHEET_NAME") from e

        print("[ИНФО] Чтение списка SKU из столбца K...")
        skus_data = sheet.get("K5:K")
        if not skus_data:
            raise ValueError("Не найдены SKU в столбце K")

        sku_values = [row[0] for row in skus_data if row and row[0]]
        total_skus = len(sku_values)
        print(f"[ИНФО] Найдено {total_skus} SKU. Начинаем групповую обработку...")

        BATCH_SIZE = 100
        ratings = []
        processed_count = 0
        error_count = 0
        index_pointer = 0  # Смещение для правильной привязки к строкам Google Sheets

        for i in range(0, total_skus, BATCH_SIZE):
            batch = sku_values[i:i+BATCH_SIZE]
            print(f"[ИНФО] Обработка SKU с {i+5} по {i+5+len(batch)-1}...")

            rating_info = get_product_ratings_batch(batch)

            # Словарь: SKU -> рейтинг
            sku_to_rating = {}
            if rating_info and "products" in rating_info:
                for product in rating_info["products"]:
                    sku_str = str(product.get("sku"))
                    rating_val = product.get("rating")
                    if rating_val is not None:
                        sku_to_rating[sku_str] = float(rating_val)

            # Заполняем результаты
            for sku in batch:
                if str(sku) in sku_to_rating:
                    ratings.append([sku_to_rating[str(sku)]])
                    processed_count += 1
                else:
                    ratings.append(["Ошибка получения рейтинга"])
                    error_count += 1

        print("\n[ИНФО] Запись рейтингов в столбец E начиная с E5...")
        if ratings:
            sheet.update(range_name=f"E5:E{5 + len(ratings) - 1}", values=ratings)
            print(f"[ГОТОВО] Успешно обновлено {processed_count} товаров.")
            print(f"[СТАТИСТИКА] Ошибок: {error_count}")

    except Exception as e:
        print(f"\n--- КРИТИЧЕСКАЯ ОШИБКА ---\n{str(e)}\n")
        if "invalid_grant" in str(e):
            print("Возможные причины:")
            print("- Неправильный файл credentials.json")
            print("- Сервисный аккаунт не имеет доступа к таблице")
            print("- Время на сервере рассинхронизировано")

if __name__ == "__main__":
    print("=== ЗАПУСК СКРИПТА ===")
    print(f"[НАСТРОЙКИ] credentials.json: {SERVICE_ACCOUNT_FILE}")
    print(f"[НАСТРОЙКИ] Client ID: {CLIENT_ID}")
    print(f"[НАСТРОЙКИ] SPREADSHEET_ID: {SPREADSHEET_ID}")
    print(f"[НАСТРОЙКИ] SHEET_NAME: {SHEET_NAME}")
    main()
    print("=== СКРИПТ ЗАВЕРШЕН ===")