import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -------- Чтение настроек --------
def read_api_credentials(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        client_id = lines[0].strip()   # Client-Id
        api_key = lines[1].strip()     # Api-Key
        sheet_id = lines[2].strip()    # ID таблицы Google Sheets
        sheet_name = lines[3].strip()  # Название листа
    return client_id, api_key, sheet_id, sheet_name

try:
    client_id, api_key, sheet_id, sheet_name = read_api_credentials("API.txt")
except UnicodeDecodeError:
    with open("API.txt", 'r', encoding='cp1251') as file:
        lines = file.readlines()
        client_id = lines[0].strip()
        api_key = lines[1].strip()
        sheet_id = lines[2].strip()
        sheet_name = lines[3].strip()

credentials_file = "credentials.json"

# -------- Доступ к Google Sheets --------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

# -------- Получаем product_id из L5:L --------
col_L_values = sheet.col_values(12)[4:]  # 12-й столбец = L, начиная с 5-й строки
product_ids = []
for pid in col_L_values:
    pid_str = (pid or "").strip()
    if not pid_str:
        product_ids.append(None)  # пустая строка: сохраняем выравнивание
        continue
    try:
        product_ids.append(int(pid_str))
    except ValueError:
        product_ids.append(None)  # нечисловое — пропускаем, но выравнивание держим

# -------- Очищаем диапазоны перед обновлением --------
if product_ids:  # есть хотя бы одна строка для обработки
    sheet.batch_clear(['BL5:BO'])  # BL, BM, BN, BO начиная с 5-й строки

# -------- Настройки API Ozon --------
api_url = "https://api-seller.ozon.ru/v4/product/info/stocks"
headers = {
    "Client-Id": client_id,
    "Api-Key": api_key,
    "Content-Type": "application/json"
}

def get_stock_data(product_ids_chunk):
    body = {
        "filter": {
            "product_id": product_ids_chunk
        },
        "limit": 1000
    }
    response = requests.post(api_url, headers=headers, json=body)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Ошибка при запросе: {response.status_code}")
        print(f"Тело ответа: {response.text}")
        return None

def update_google_sheet(data, raw_product_ids):
    # product_id -> список stocks
    stock_dict = {str(item.get('product_id')): item.get('stocks', []) for item in data.get('items', [])}

    rows_bl_fbs_present = []   # BL: только FBS present
    rows_bm_reserved_all = []  # BM: резерв общий (fbo+fbs)
    rows_bn_reserved_fbo = []  # BN: резерв FBO
    rows_bo_reserved_fbs = []  # BO: резерв FBS

    for pid in raw_product_ids:
        if pid is None:
            rows_bl_fbs_present.append([""])
            rows_bm_reserved_all.append([""])
            rows_bn_reserved_fbo.append([""])
            rows_bo_reserved_fbs.append([""])
            continue

        stocks = stock_dict.get(str(pid), [])

        fbs_present = 0
        total_reserved = 0
        reserved_fbo = 0
        reserved_fbs = 0

        for s in stocks:
            t = s.get('type')
            present = int(s.get('present') or 0)
            reserved = int(s.get('reserved') or 0)

            if t == 'fbs':
                fbs_present += present
                reserved_fbs += reserved
            elif t == 'fbo':
                reserved_fbo += reserved

            total_reserved += reserved  # общий резерв по всем типам

        rows_bl_fbs_present.append([fbs_present])
        rows_bm_reserved_all.append([total_reserved])
        rows_bn_reserved_fbo.append([reserved_fbo])
        rows_bo_reserved_fbs.append([reserved_fbs])

    start_row = 5
    end_row = start_row + len(rows_bl_fbs_present) - 1
    if rows_bl_fbs_present:
        sheet.update(range_name=f'BL{start_row}:BL{end_row}', values=rows_bl_fbs_present)
        sheet.update(range_name=f'BM{start_row}:BM{end_row}', values=rows_bm_reserved_all)
        sheet.update(range_name=f'BN{start_row}:BN{end_row}', values=rows_bn_reserved_fbo)
        sheet.update(range_name=f'BO{start_row}:BO{end_row}', values=rows_bo_reserved_fbs)

# -------- Запросы к API чанками --------
valid_ids = [pid for pid in product_ids if isinstance(pid, int)]
chunks = [valid_ids[i:i + 1000] for i in range(0, len(valid_ids), 1000)] if valid_ids else []

final_data = {'items': []}
for chunk in chunks:
    stock_data = get_stock_data(chunk)
    if stock_data and 'items' in stock_data:
        final_data['items'].extend(stock_data['items'])

# -------- Обновляем таблицу --------
if final_data['items']:
    update_google_sheet(final_data, product_ids)
    print("Готово: BL(FBS present), BM(reserved all), BN(reserved FBO), BO(reserved FBS).")
else:
    # Нет данных от API — после очистки столбцы уже пустые
    print("Нет данных от API. Диапазон BL5:BO очищен.")
