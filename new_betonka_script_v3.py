import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import xlsxwriter
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
import time

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

pd.set_option('display.max_columns', None)
"""Подключаемся и селектим артикула список артикулов с нужными атрибутами"""
conn = psycopg2.connect(dbname="desport",
                        host="10.61.32.20", 
                        user="anikeshin",
                        password="7gtTHMOU6xIhX95zEx71V9aVJPo",
                        port="6432")
cur = conn.cursor()
cur.execute("""SELECT sector_name, family_name, modelcode, skuname, skucode, dec_productlifecycle, dec_producttype
            FROM cds.d_product""")
res = cur.fetchall()
df_tree = pd.DataFrame(res, columns=['sector', 'family', 'model_code', 'sku_name', 'sku_code', 'life_stage', 'dec_producttype'])

"""Селектим продажи"""
cur.execute(f"""SELECT item, sum(turnover), sum(qty_sum)
                FROM dev.all_checks
                WHERE day_id BETWEEN '{datetime.today().date() - timedelta(days=14)}' AND '{datetime.today().date() - timedelta(days=1)}'
                GROUP BY item""")
res = cur.fetchall()
df_turnover = pd.DataFrame(res, columns=['sku_code', 'turnover','qty_sales'])

"""Селектим сток магазинов"""
cur.execute("""select item, store, qty
                from dev.stock_store
                where date = (select max(date) from dev.stock_store)""")
res = cur.fetchall()
df_stock_store = pd.DataFrame(res, columns=['sku_code', 'store', 'store_qty'])

"""Селектим сток склада"""
cur.execute("""select item, qty
                from dev.stock_wh_available
                where date = (select max(date) from dev.stock_store)""")
res = cur.fetchall()
df_stock_wh = pd.DataFrame(res, columns=['sku_code', 'qty'])

"""Селектим рейндж"""
cur.execute("""select dp.skucode, 
                      dp.productcategory as range
                from cds.d_product dp """)
res = cur.fetchall()
df_range = pd.DataFrame(res, columns=['sku_code', 'range'])

"""Селектим входящие заказы"""
cur.execute("""select item, store, incoming_qty
                from dev.stock_store_incoming
                where date = (select max(date) from dev.stock_store)""")
res = cur.fetchall()
df_stock_orders = pd.DataFrame(res, columns=['sku_code', 'store', 'order_qty'])

##Селектим национальные цены
cur.execute("""SELECT skucode as sku,
            price as national_price
            FROM cds.f_prices_retail
            Where price_type='Национальная регулярная'
            and now()::date between date_start and date_end""")
res = cur.fetchall()
df_national_prices = pd.DataFrame(res, columns=['sku_code', 'national_price'])

##Селектим цены по карте лояльности
cur.execute("""SELECT skucode as sku,
            price as loyalty_price
            FROM cds.f_prices_retail
            Where price_type='Цена по карте лояльности'
            and now()::date between date_start and date_end""")
res = cur.fetchall()
df_loyalty_prices = pd.DataFrame(res, columns=['sku_code', 'loyalty_price'])

#Вытягиваем производственный тип

cur.execute("""select skucode as sku_code,
	      dec_modelproductionattribution as production_type
            from cds.d_product""")
res = cur.fetchall()
df_production_type = pd.DataFrame(res, columns=['sku_code', 'production_type'])


"""Получаем список магазинов (магазины со стоком или с заказами)"""
df_stores = pd.concat([df_stock_store[['store']], df_stock_orders[['store']]]).drop_duplicates().sort_values('store')

"""Находим текущую неделю и год"""
current_week = datetime.now().isocalendar().week
current_year = datetime.now().isocalendar().year
"""Селектим прогноз будущих 4 недель"""
cur.execute(f"""select item, sum(forecast)
                from dev.supply_forecast
                where week >= '{current_week}' and week <= '{current_week + 3}' and year = {current_year}
                group by item""")
res = cur.fetchall()
df_forecast = pd.DataFrame(res, columns=['sku_code', 'forecast'])
df_forecast['forecast'] = df_forecast['forecast'].apply(pd.to_numeric, downcast='float')

"""Селектим из готовой вьюшки долю продаж магазинов за последние 30 дней"""
cur.execute("""select store_name, store_share
                from dev.store_share_last_30_days""")
res = cur.fetchall()
df_store_shares = pd.DataFrame(res, columns=['store', 'share'])

conn.close()


"""Из датафрейма создаем словарь. Ключ - магазин, значение - доля. Зачем не понимаю, вероятно skill shortage на момент написания кода"""
store_shares = {}
for index, row in df_store_shares.iterrows():
    store_shares[row['store']] = float(row['share'])
# if 'Пулково' not in store_shares.keys():
#     store_shares['Пулково'] = 0.07

"""Создаем список артикул для которых сток есть где-либо."""
df1 = df_stock_store[['sku_code']].drop_duplicates(subset=['sku_code'])
df2 = df_stock_wh[['sku_code']].drop_duplicates(subset=['sku_code'])
df3 = df_stock_orders[['sku_code']].drop_duplicates(subset=['sku_code'])
df = pd.concat([df_stock_store[['sku_code']], df_stock_wh[['sku_code']], df_stock_orders[['sku_code']]]).drop_duplicates()

"""Добавляем артибуты иерархии"""
df = pd.merge(df, df_tree, how='inner', on='sku_code')
"""Добавляем продажи"""
df = pd.merge(df, df_turnover, how='left', on='sku_code')
df[['turnover']] = df[['turnover']].fillna(0)
df[['qty_sales']] = df[['qty_sales']].fillna(0)
"""Добавляем сток"""
df = pd.merge(df, df_stock_wh, how='left', on='sku_code')
df[['qty']] = df[['qty']].fillna(0)
"""Добавляем гамму"""
df = pd.merge(df, df_range, how='left', on='sku_code')
df[['range']] = df[['range']].fillna('unknown')

"""Добавляем национальные цены"""
df = pd.merge(df, df_national_prices, how='left', on='sku_code')
df[['national_price']] = df[['national_price']].fillna(0)

"""Добавляем национальные цены"""
df = pd.merge(df, df_loyalty_prices, how='left', on='sku_code')
df[['loyalty_price']] = df[['loyalty_price']].fillna(0)

"""Добавляем производственный тип"""
df = pd.merge(df, df_production_type, how='left', on='sku_code')
df[['production_type']] = df[['production_type']].fillna(0)


"""Добавляем пустые колонки для стока магазинов и кол-ва алертов, а также колонку, значения которой будут чекбоксом в файле"""
df['store stock'] = ''
df['store order'] = ''
df['indicator'] = False
df = df[['indicator','sector', 'family', 'model_code', 'sku_name', 'sku_code', 'turnover','qty_sales', 'life_stage',
         'dec_producttype', 'range', 'qty', 'store stock', 'store order', 'national_price', 'loyalty_price', 'production_type']]

# Без продаж (нужно удалить строку выше и раскомментить эту)
# df = df[['indicator', 'sector', 'family', 'model_code', 'sku_name', 'sku_code', 'life_stage',
#          'dec_producttype', 'range', 'qty', 'store stock', 'Всего алертов']]

# Строки только со стоком на складе (нужно просто раскомментить строку ниже)
# df = df[df['qty'] > 0]

"""Словарь для переименования колонок датафрейма"""
my_dict = {'sector': 'Сектор',
           'family': 'Семья',
           'model_code': 'Код модели',
           'sku_name': 'Название модели',
           'sku_code': 'Артикул',
           'turnover': 'ТО за 2 недели',
           'qty_sales' : 'Кол-ва шт. за 2 недели',
           'life_stage': 'ЖЦТ',
           'qty': 'Сток на складе',
           'store stock': 'Сток магазинов',
           'store order' : 'Сток в пути',
           'range': 'Гамма',
           'indicator': 'Индикатор',
           'dec_producttype' : 'Тип продукта',
           'national_price' : 'Национальная регулярная цена',
           'loyalty_price' : 'Цена по карте лояльности',
           'production_type' : 'Производственный тип'}

"""Цикл для по магазинам"""
for index, row in df_stores.iterrows():
    """Подтягиваем сток данного магазина"""
    df = pd.merge(df, df_stock_store[df_stock_store['store'] == row['store']][['sku_code', 'store_qty']], how='left',
                  on='sku_code')
    df['store_qty'] = df['store_qty'].fillna(0)
    """Меняем название столбца, чтобы оно было уникальным с отсылом к номеру магазина"""
    df = df.rename(columns={'store_qty': row['store'] + ' Сток'})

    """Добавляем заказы"""
    df = pd.merge(df, df_stock_orders[df_stock_orders['store'] == row['store']][['sku_code', 'order_qty']], how='left',
                  on='sku_code')
    df['order_qty'] = df['order_qty'].fillna(0)
    df = df.rename(columns={'order_qty': row['store'] + ' В пути'})


df['turnover']=df['turnover'].astype(int)
df['qty_sales']=df['qty_sales'].astype(int)
df['national_price']=df['national_price'].astype(int)
df['loyalty_price']=df['loyalty_price'].astype(int)

for index, row in df.iterrows():
    total_store_stock = 0  # Инициализация для каждой строки df
    for store_index, store_row in df_stores.iterrows():
        store_stock = row.get(store_row['store'] + ' Сток', 0)  # Используем get для безопасного доступа
        if store_stock < 0:
            store_stock = 0
        total_store_stock += store_stock  # Суммируем стоки
    df.at[index, 'store stock'] = total_store_stock  # Записываем итоговое значение


for index, row in df.iterrows():
    total_store_order = 0  # Инициализация для каждой строки df
    for store_index, store_row in df_stores.iterrows():
        # Используем get для безопасного доступа к значению
        store_order = row.get(store_row['store'] + ' В пути', 0)  
        if store_order < 0:
            store_order = 0
        total_store_order += store_order  # Суммируем заказы
    df.at[index, 'store order'] = total_store_order  # Записываем итоговое значение


"""Далее нам нужно создать первую строку датафрейма с датой обновления и названиями магазина. А в столбцах, чтобы название магазина не повторять."""
first_row = []
second_row = []
store = ''
for item in list(df):
    if item.find(' Сток') > 0:
        first_row.append(item[:item.find(' Сток')])
        store = item[:item.find(' Сток')]
    else:
        first_row.append('')
    if store != '':
        second_row.append(item.replace(f"{store} ", ''))
    else:
        if item in my_dict:
            second_row.append(my_dict[item])
        else:
            second_row.append(item)
first_row[1] = f"Updated on {datetime.now().strftime('%d.%m.%Y')} at {datetime.now().strftime('%H:%M:%S')}"
"""Конкатим 2 эти строки"""
output_list = [first_row, second_row]
"""И к ним конкатим основной датафрейм"""
output_list = output_list + df.values.tolist()

# Выгрузка в эксель файл

df = pd.DataFrame(output_list)
with pd.ExcelWriter('betonka.xlsx') as writer:
    df.to_excel(writer, sheet_name='Сток Бетонка', header=True, index=False)

# Гугл часть -------------------------------------

# Параметры для работы API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'service.json'
SAMPLE_RANGE_NAME = 'Сток!A5:CA20000'
SAMPLE_RANGE_NAME_DELETE = 'Сток!B5:CA20000'
STOCK_ALERT = '1oIyZn2x8MZ7kyEDxM66jayb_L1vvfFJCxaBvpQKy2vE'

MAX_RETRIES = 3
RETRY_DELAY = 2

class GoogleSheetsClient:
    def __init__(self, service_account_file, scopes):
        self.creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
        self.service = self._create_service()

    def _create_service(self):
        for attempt in range(MAX_RETRIES):
            try:
                # Создаем сессию с таймаутом
                session = Session()
                retry = Retry(total=MAX_RETRIES, backoff_factor=RETRY_DELAY)
                adapter = HTTPAdapter(max_retries=retry)
                session.mount('http://', adapter)
                session.mount('https://', adapter)

                # Создаем сервис без передачи http
                return build('sheets', 'v4', credentials=self.creds, cache_discovery=False)
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(RETRY_DELAY)
        raise Exception("Failed to create Google Sheets service after multiple attempts.")

    def clean(self, spreadsheet_id, range_name):
        try:
            body = {}
            result = self.service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range_name, body=body).execute()
            return result
        except HttpError as error:
            print(f"An error occurred while clearing values: {error}")
            return None

    def update_values(self, spreadsheet_id, range_name, value_input_option, values):
        try:
            body = {
                'values': values
            }
            result = self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=range_name,
                valueInputOption=value_input_option, body=body).execute()
            print(f"{result.get('updatedCells')} cells updated.")
            return result
        except HttpError as error:
            print(f"An error occurred while updating values: {error}")
            return None

# Создаем экземпляр клиента
try:
    client = GoogleSheetsClient(SERVICE_ACCOUNT_FILE, SCOPES)
    client.clean(STOCK_ALERT, SAMPLE_RANGE_NAME_DELETE)
    client.update_values(STOCK_ALERT, SAMPLE_RANGE_NAME, "RAW", output_list)
    print('Calculation has been finished')
except Exception as e:
    print(f"An error occurred: {e}")


