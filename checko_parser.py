import os
import time
import random
from datetime import datetime
import requests
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import schedule
import dotenv
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

dotenv.load_dotenv()

# Конфигурация
API_KEY = os.getenv('API_KEY')  # API ключ для rucaptcha
SMTPBZ_API_KEY = os.getenv('SMTPBZ_API_KEY')  # API ключ для smtp.bz
BASE_URL = "https://checko.ru"
START_PAGE = 1
END_PAGE = 10
OUTPUT_FILE = "companies_data.xlsx"
PAGE_LOAD_TIMEOUT = 60
MAX_RETRIES = 10  # Максимальное количество попыток для решения капчи

# Настройки email рассылки
EMAIL_CONFIG = {
    'from_email': 'sale@warmcustomers.ru',
    'from_name': 'Ирина Бондаренко',
    'subject': 'Клиенты за 50₽',
    'html_content': """
    <html>
    <body>
        <p>Здравствуйте!</p>
        <p>Мы ранее обсуждали парсинг входящих и исходящих звонков с номеров отдела продаж Ваших конкурентов для сбора горячих лидов. К сожалению, я потеряла Ваш номер, но Ваш e-mail сохранился.</p>
        <p>Подскажите, актуален ли для Вас этот вопрос?</p>
        <p>С уважением, Ирина менеджер компании Hot Clients<br>
        Телефон +7 495 128-15-51<br>
        WhatsApp +7 909 696-04-44<br>
        Telegram @Hotclient<br>
        Сайт <a href="http://hot-clients.ru">http://hot-clients.ru</a></p>
    </body>
    </html>
    """,
    'text_content': """
    Здравствуйте!

    Мы ранее обсуждали парсинг входящих и исходящих звонков с номеров отдела продаж Ваших конкурентов для сбора горячих лидов. К сожалению, я потеряла Ваш номер, но Ваш e-mail сохранился.

    Подскажите, актуален ли для Вас этот вопрос?

    С уважением, Ирина менеджер компании Hot Clients
    Телефон +7 495 128-15-51
    WhatsApp +7 909 696-04-44
    Telegram @Hotclient
    Сайт http://hot-clients.ru
    """,
    'tag': 'hot_clients_campaign'
}


def debug_screenshot(driver, name):
    """Сохранение скриншота для отладки"""
    if not os.path.exists('debug'):
        os.makedirs('debug')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    path = f'debug/{name}_{timestamp}.png'
    driver.save_screenshot(path)
    logger.debug(f"Скриншот сохранен: {path}")


def solve_recaptcha_v2(driver):
    """Полное решение reCAPTCHA v2 с отладкой"""
    print("Начинаем решение reCAPTCHA v2...")
    debug_screenshot(driver, "before_solving")

    try:
        sitekey = driver.find_element(By.CSS_SELECTOR, 'div[data-sitekey]').get_attribute("data-sitekey")
        pageurl = driver.current_url
        print(f"Sitekey: {sitekey}, URL: {pageurl}")

        payload = {
            "clientKey": API_KEY,
            "task": {
                "type": "RecaptchaV2TaskProxyless",
                "websiteURL": pageurl,
                "websiteKey": sitekey,
                "isInvisible": False
            },
            "softId": 3898
        }

        response = requests.post(
            "https://api.rucaptcha.com/createTask",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        result = response.json()

        if result.get("errorId") != 0:
            error = result.get("errorDescription", "Неизвестная ошибка API")
            raise Exception(f"Ошибка API при создании задачи: {error}")

        task_id = result["taskId"]
        print(f"Задача создана, ID: {task_id}")
        debug_screenshot(driver, "task_created")

        start_time = time.time()
        while time.time() - start_time < 300:
            time.sleep(10)

            status_response = requests.post(
                "https://api.rucaptcha.com/getTaskResult",
                json={"clientKey": API_KEY, "taskId": task_id},
                headers={"Content-Type": "application/json"},
                timeout=30
            ).json()

            print(f"Статус решения: {json.dumps(status_response, indent=2)}")

            if status_response.get("status") == "ready":
                token = status_response["solution"]["gRecaptchaResponse"]
                print("Капча успешно решена!")

                driver.execute_script(f"""
                    var response = document.getElementById('g-recaptcha-response');
                    if (response) {{
                        response.style.display = '';
                        response.value = '{token}';
                    }} else {{
                        var input = document.createElement('input');
                        input.type = 'hidden';
                        input.id = 'g-recaptcha-response';
                        input.name = 'g-recaptcha-response';
                        input.value = '{token}';
                        document.body.appendChild(input);
                    }}
                """)
                debug_screenshot(driver, "after_token_input")
                time.sleep(2)

                submit_btn = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "button[type='submit']"))
                )
                driver.execute_script("arguments[0].click();", submit_btn)
                print("Форма отправлена через JS")
                debug_screenshot(driver, "after_submit")
                time.sleep(3)

                return True

            elif status_response.get("errorId") != 0:
                error = status_response.get("errorDescription", "Неизвестная ошибка API")
                raise Exception(f"Ошибка API: {error}")

        raise Exception("Превышено время ожидания решения (5 минут)")

    except Exception as e:
        debug_screenshot(driver, "captcha_error")
        print(f"Ошибка при решении капчи: {str(e)}")
        return False


def setup_driver():
    """Настройка веб-драйвера с уникальным каталогом данных"""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    user_data_dir = f"/tmp/chrome_{int(time.time())}"
    options.add_argument(f"--user-data-dir={user_data_dir}")

    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            """
        })
        return driver
    except Exception as e:
        print(f"Ошибка при создании драйвера: {str(e)}")
        raise


def handle_captcha(driver):
    """Полная обработка капчи с улучшенной логикой"""
    print("Обнаружена капча, начинаем обработку...")
    debug_screenshot(driver, "captcha_detected")

    try:
        checkbox_frame = WebDriverWait(driver, 20).until(
            EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "iframe[title*='reCAPTCHA']"))
        )
        checkbox = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "recaptcha-checkbox"))
        )
        checkbox.click()
        print("Чекбокс 'Я не робот' нажат")
        driver.switch_to.default_content()
        debug_screenshot(driver, "after_checkbox_click")
        time.sleep(3)

        if not solve_recaptcha_v2(driver):
            return False

        return True

    except Exception as e:
        debug_screenshot(driver, "captcha_handling_error")
        print(f"Ошибка при обработке капчи: {str(e)}")
        return False


def get_all_company_links(driver):
    """Сбор ссылок на компании с обработкой ошибок"""
    all_links = []
    existing_links = set()

    # Загружаем уже сохраненные ссылки, чтобы избежать дублирования
    if os.path.exists(OUTPUT_FILE):
        try:
            df = pd.read_excel(OUTPUT_FILE)
            if 'URL' in df.columns:
                existing_links = set(df['URL'].dropna().unique())
                logger.info(f"Загружено {len(existing_links)} существующих ссылок из файла")
        except Exception as e:
            logger.error(f"Ошибка при загрузке существующих ссылок: {str(e)}")

    for page in range(START_PAGE, END_PAGE + 1):
        logger.info(f"Обработка страницы {page} из {END_PAGE}")
        url = f"{BASE_URL}/company/updates?page={page}"

        try:
            # Загрузка страницы с повторными попытками
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    driver.get(url)
                    WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
                        lambda d: d.find_elements(By.CSS_SELECTOR, "table.table") or
                                  d.find_elements(By.CSS_SELECTOR, "iframe[title*='reCAPTCHA']")
                    )
                    break
                except Exception as e:
                    if attempt == MAX_RETRIES:
                        raise
                    logger.warning(f"Попытка {attempt}: Ошибка загрузки страницы {page}: {str(e)}")
                    time.sleep(5 * attempt)

            debug_screenshot(driver, f"page_{page}")

            # Обработка капчи, если появилась
            if driver.find_elements(By.CSS_SELECTOR, "iframe[title*='reCAPTCHA']"):
                if not handle_captcha(driver):
                    logger.warning(f"Не удалось обойти капчу на странице {page}")
                    continue

            # Парсинг ссылок
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            page_links = []

            for a in soup.select('a.link[href^="/company/"]'):
                full_url = BASE_URL + a['href'] if a['href'].startswith('/') else a['href']
                if full_url not in existing_links:  # Проверка на дубликаты
                    page_links.append(full_url)

            all_links.extend(page_links)
            logger.info(f"Найдено {len(page_links)} новых ссылок на странице {page}")

            # Случайная задержка между страницами
            time.sleep(random.uniform(3, 10))

        except Exception as e:
            logger.error(f"Ошибка при обработке страницы {page}: {str(e)}")
            debug_screenshot(driver, f"error_page_{page}")
            continue

    return all_links



def parse_company_page(driver, url):
    """Парсинг данных компании с улучшенной обработкой телефонов и email"""
    print(f"\nОбрабатываем компанию: {url}")
    try:
        driver.get(url)
        debug_screenshot(driver, f"company_page_{url.split('/')[-1]}")

        WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
            lambda d: d.find_elements(By.ID, "copy-inn") or
                      d.find_elements(By.CSS_SELECTOR, "iframe[title*='reCAPTCHA']")
        )

        if driver.find_elements(By.CSS_SELECTOR, "iframe[title*='reCAPTCHA']"):
            if not handle_captcha(driver):
                return None

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "copy-inn"))
        )

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        inn = soup.find('strong', id='copy-inn').get_text(strip=True) if soup.find('strong', id='copy-inn') else None
        date = soup.find('div', string='Дата регистрации').find_next('div').get_text(strip=True) if soup.find('div',
                                                                                                              string='Дата регистрации') else None

        phone_section = soup.find('strong', string='Телефоны')
        phones = []
        if phone_section:
            for a in phone_section.find_next('div').find_all('a', class_='link-pseudo',
                                                             href=lambda x: x and x.startswith('tel:')):
                phones.append(a.get_text(strip=True))
        phone = ', '.join(phones) if phones else None

        email_tag = soup.find('a', href=lambda x: x and x.startswith('mailto:'))
        email = email_tag.get_text(strip=True) if email_tag else None

        if not inn:
            print("Пропускаем - нет ИНН")
            return None

        if not phone and not email:
            print("Пропускаем - нет ни телефона, ни email")
            return None

        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Данные: ИНН={inn}, Дата={date}, Телефон={phone}, Email={email}")
        return [inn, date, phone, email, url, current_date]

    except Exception as e:
        debug_screenshot(driver, f"parse_error_{url.split('/')[-1]}")
        print(f"Ошибка при парсинге компании: {str(e)}")
        return None


def load_existing_data(filepath):
    """Загрузка существующих данных из файла"""
    if os.path.exists(filepath):
        try:
            df = pd.read_excel(filepath)
            return df
        except Exception as e:
            print(f"Ошибка при загрузке файла: {str(e)}")
            return pd.DataFrame()
    return pd.DataFrame()


def save_to_excel(new_data, filepath):
    """Сохранение данных с улучшенной проверкой дубликатов"""
    try:
        # Загрузка существующих данных
        existing_df = load_existing_data(filepath)

        # Создание DataFrame из новых данных
        new_df = pd.DataFrame(new_data,
                              columns=['ИНН', 'Дата регистрации', 'Телефон', 'Email', 'URL', 'Дата добавления'])

        # Удаление полностью пустых строк
        new_df = new_df.dropna(how='all')

        # Фильтрация только компаний с телефоном или email
        new_df = new_df[(new_df['Телефон'].notna()) | (new_df['Email'].notna())]

        if not existing_df.empty:
            # Объединение с обновлением информации
            final_df = pd.concat([existing_df, new_df]).drop_duplicates(
                subset=['ИНН'],
                keep='last'  # Сохраняем последнюю версию данных
            )
        else:
            final_df = new_df.drop_duplicates(subset=['ИНН'])

        # Сохранение результата
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False)

            # Форматирование столбцов
            worksheet = writer.sheets['Sheet1']
            worksheet.set_column('A:A', 15)  # ИНН
            worksheet.set_column('B:B', 15)  # Дата регистрации
            worksheet.set_column('C:C', 20)  # Телефон
            worksheet.set_column('D:D', 25)  # Email
            worksheet.set_column('E:E', 40)  # URL
            worksheet.set_column('F:F', 20)  # Дата добавления

        logger.info(f"Данные сохранены. Всего записей: {len(final_df)} (добавлено {len(new_df)})")

    except Exception as e:
        logger.error(f"Ошибка при сохранении: {str(e)}")
        raise


def send_emails_via_smtpbz(emails_data):
    """Отправка электронных писем через сервис smtp.bz"""
    url = "https://api.smtp.bz/v1/smtp/send"
    headers = {
        "Authorization": SMTPBZ_API_KEY,
        "Content-Type": "application/json"
    }

    try:
        # Отправляем каждому получателю отдельно
        for email_info in emails_data:
            payload = {
                "from": EMAIL_CONFIG['from_email'],
                "name": EMAIL_CONFIG['from_name'],
                "subject": EMAIL_CONFIG['subject'],
                "to": email_info['email'],
                "html": EMAIL_CONFIG['html_content'],
                "text": EMAIL_CONFIG['text_content']
            }

            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response_data = response.json()

            # Проверяем ответ сервера
            if response.status_code == 200:
                if response_data.get('result'):
                    print(f"Письмо для {email_info['email']} успешно отправлено!")
                else:
                    error = response_data.get('message', 'Неизвестная ошибка API')
                    return False, f"Ошибка для {email_info['email']}: {error}"
            else:
                return False, f"HTTP ошибка {response.status_code}: {response.text}"

        return True, "Все письма успешно отправлены"

    except Exception as e:
        return False, f"Ошибка подключения: {str(e)}"



def process_and_send_emails(filepath):
    """
    Обработка данных и отправка писем компаниям с email

    :param filepath: Путь к файлу с данными компаний
    """
    if not os.path.exists(filepath):
        print("Файл с данными не найден")
        return

    try:
        df = pd.read_excel(filepath)

        # Фильтруем компании с email и без отметки об отправке
        if 'EmailSent' in df.columns:
            email_companies = df[(df['Email'].notna()) & (df['EmailSent'].isna())]
        else:
            email_companies = df[df['Email'].notna()]
            df['EmailSent'] = None

        if email_companies.empty:
            print("Нет компаний с email для отправки")
            return

        print(f"Найдено {len(email_companies)} компаний с email для отправки")

        # Подготовка данных для отправки
        emails_data = []
        for _, row in email_companies.iterrows():
            emails_data.append({
                "email": row['Email'],
                "name": row.get('ИНН', '')
            })

        # Отправка писем
        success, message = send_emails_via_smtpbz(emails_data)
        print(f"Результат отправки: {message}")

        # Обновляем статус отправки
        if success:
            sent_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df.loc[email_companies.index, 'EmailSent'] = sent_date

            with pd.ExcelWriter(filepath, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                df.to_excel(writer, index=False)

            print(f"Обновлен статус отправки для {len(email_companies)} компаний")

    except Exception as e:
        print(f"Ошибка при обработке и отправке писем: {str(e)}")


def job():
    """Основная задача парсера с улучшенной логикой"""
    logger.info(f"=== Запуск парсера {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

    driver = None
    try:
        driver = setup_driver()

        # Получаем все ссылки на компании
        company_links = get_all_company_links(driver)
        logger.info(f"Найдено {len(company_links)} компаний для обработки")

        # Загружаем существующие данные для проверки дубликатов
        existing_data = load_existing_data(OUTPUT_FILE)
        existing_inns = set(existing_data['ИНН'].tolist()) if not existing_data.empty else set()

        all_data = []
        processed_count = 0

        # Парсинг компаний
        for i, link in enumerate(company_links, 1):
            logger.info(f"Обработка компании {i}/{len(company_links)}: {link}")

            data = parse_company_page(driver, link)
            if data and data[0] not in existing_inns:  # Проверка на дубликаты по ИНН
                all_data.append({
                    'ИНН': data[0],
                    'Дата регистрации': data[1],
                    'Телефон': data[2],
                    'Email': data[3],
                    'URL': data[4],
                    'Дата добавления': data[5],
                    'EmailSent': None  # Добавляем колонку для отметки об отправке
                })
                processed_count += 1
                existing_inns.add(data[0])  # Добавляем ИНН в множество

                # Промежуточное сохранение каждые 10 компаний
                if i % 10 == 0:
                    logger.info(f"Промежуточное сохранение после {i} компаний...")
                    save_to_excel(all_data, OUTPUT_FILE)

            # Случайная задержка между запросами
            time.sleep(random.uniform(2, 5))

        # Финальное сохранение
        logger.info("Финальное сохранение результатов...")
        save_to_excel(all_data, OUTPUT_FILE)

        # Отправка писем
        logger.info("Начинаем отправку писем...")
        process_and_send_emails(OUTPUT_FILE)

        logger.info(f"\n=== Итоги ===")
        logger.info(f"Обработано компаний: {len(company_links)}")
        logger.info(f"Добавлено новых записей: {processed_count}")
        logger.info(f"Всего записей в файле: {len(existing_inns) + processed_count}")

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        if driver:
            debug_screenshot(driver, "critical_error")
    finally:
        if driver:
            driver.quit()
        logger.info("=== Завершение работы ===")


def run_scheduler():
    """Запуск планировщика с обработкой прерываний"""
    job()  # Запуск при старте

    schedule.every().day.at("08:00").do(job)

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("Планировщик остановлен")


if __name__ == "__main__":
    run_scheduler()