import os
import time
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

dotenv.load_dotenv()


# Конфигурация
# Конфигурация
API_KEY = os.getenv('API_KEY')  # Замените на реальный ключ
BASE_URL = "https://checko.ru"
START_PAGE = 1
END_PAGE = 10  # Всего 10 страниц
OUTPUT_FILE = "companies_data.xlsx"
PAGE_LOAD_TIMEOUT = 60

def debug_screenshot(driver, name):
    """Сохранение скриншота для отладки"""
    if not os.path.exists('debug'):
        os.makedirs('debug')
    driver.save_screenshot(f'debug/{name}.png')


def solve_recaptcha_v2(driver):
    """Полное решение reCAPTCHA v2 с отладкой"""
    print("Начинаем решение reCAPTCHA v2...")
    debug_screenshot(driver, "before_solving")

    try:
        # Получаем параметры капчи
        sitekey = driver.find_element(By.CSS_SELECTOR, 'div[data-sitekey]').get_attribute("data-sitekey")
        pageurl = driver.current_url
        print(f"Sitekey: {sitekey}, URL: {pageurl}")

        # 1. Создаем задачу в API
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

        # 2. Ожидаем решения
        start_time = time.time()
        while time.time() - start_time < 300:  # 5 минут максимум
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

                # 3. Вводим токен
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

                # 4. Нажимаем кнопку через JS
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

    # Основные настройки
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # Для работы на сервере
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Уникальный каталог данных для каждой сессии
    user_data_dir = f"/tmp/chrome_{int(time.time())}"
    options.add_argument(f"--user-data-dir={user_data_dir}")

    # Улучшенный User-Agent
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
        # 1. Кликаем на чекбокс "Я не робот"
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

        # 2. Решаем капчу через API
        if not solve_recaptcha_v2(driver):
            return False

        return True

    except Exception as e:
        debug_screenshot(driver, "captcha_handling_error")
        print(f"Ошибка при обработке капчи: {str(e)}")
        return False


def get_all_company_links(driver):
    """Собираем ссылки на компании со всех страниц"""
    all_links = []

    for page in range(START_PAGE, END_PAGE + 1):
        print(f"\nОбрабатываем страницу {page} из {END_PAGE}")
        url = f"{BASE_URL}/company/updates?page={page}"

        try:
            driver.get(url)
            debug_screenshot(driver, f"page_{page}")

            # Ожидаем загрузки
            WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, "table.table") or
                          d.find_elements(By.CSS_SELECTOR, "iframe[title*='reCAPTCHA']")
            )

            # Обработка капчи, если появилась
            if driver.find_elements(By.CSS_SELECTOR, "iframe[title*='reCAPTCHA']"):
                if not handle_captcha(driver):
                    print(f"Не удалось обойти капчу на странице {page}")
                    continue

            # Парсим ссылки
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            page_links = []

            for a in soup.select('a.link[href^="/company/"]'):
                full_url = BASE_URL + a['href'] if a['href'].startswith('/') else a['href']
                page_links.append(full_url)

            all_links.extend(page_links)
            print(f"Найдено {len(page_links)} ссылок на странице {page}")

            # Пауза между страницами
            time.sleep(5)

        except Exception as e:
            debug_screenshot(driver, f"error_page_{page}")
            print(f"Ошибка при обработке страницы {page}: {str(e)}")
            continue

    return all_links


def parse_company_page(driver, url):
    """Парсинг данных компании с улучшенной обработкой телефонов и email"""
    print(f"\nОбрабатываем компанию: {url}")
    try:
        driver.get(url)
        debug_screenshot(driver, f"company_page_{url.split('/')[-1]}")

        # Ожидаем либо данные, либо капчу
        WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
            lambda d: d.find_elements(By.ID, "copy-inn") or
                      d.find_elements(By.CSS_SELECTOR, "iframe[title*='reCAPTCHA']")
        )

        # Если есть капча - обрабатываем
        if driver.find_elements(By.CSS_SELECTOR, "iframe[title*='reCAPTCHA']"):
            if not handle_captcha(driver):
                return None

        # Дожидаемся загрузки данных
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "copy-inn"))
        )

        # Парсинг данных
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Основные данные
        inn = soup.find('strong', id='copy-inn').get_text(strip=True) if soup.find('strong', id='copy-inn') else None
        date = soup.find('div', string='Дата регистрации').find_next('div').get_text(strip=True) if soup.find('div',
                                                                                                              string='Дата регистрации') else None

        # Телефоны (собираем все номера через запятую)
        phone_section = soup.find('strong', string='Телефоны')
        phones = []
        if phone_section:
            for a in phone_section.find_next('div').find_all('a', class_='link-pseudo',
                                                             href=lambda x: x and x.startswith('tel:')):
                phones.append(a.get_text(strip=True))
        phone = ', '.join(phones) if phones else None

        # Email
        email_tag = soup.find('a', href=lambda x: x and x.startswith('mailto:'))
        email = email_tag.get_text(strip=True) if email_tag else None

        # Проверяем обязательные поля
        if not inn:
            print("Пропускаем - нет ИНН")
            return None

        if not phone and not email:
            print("Пропускаем - нет ни телефона, ни email")
            return None

        # Формируем строку для таблицы
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
    """Добавление новых данных в существующий файл с проверкой дубликатов"""
    try:
        # Загружаем существующие данные
        existing_df = load_existing_data(filepath)

        # Создаем DataFrame из новых данных
        new_df = pd.DataFrame(new_data,
                              columns=['ИНН', 'Дата регистрации', 'Телефон', 'Email', 'URL', 'Дата добавления'])

        # Фильтруем новые данные: оставляем только записи с телефоном или email
        new_df = new_df[(new_df['Телефон'].notna()) | (new_df['Email'].notna())]

        # Удаляем дубликаты ИНН (если они есть в новых данных)
        new_df = new_df.drop_duplicates(subset=['ИНН'])

        if not existing_df.empty:
            # Удаляем из новых данных записи, которые уже есть в файле
            existing_inns = existing_df['ИНН'].unique()
            new_df = new_df[~new_df['ИНН'].isin(existing_inns)]

            # Объединяем старые и новые данные
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            final_df = new_df

        # Сохраняем результат
        with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False)

            # Настройка ширины столбцов
            worksheet = writer.sheets['Sheet1']
            worksheet.set_column('A:A', 15)  # ИНН
            worksheet.set_column('B:B', 15)  # Дата регистрации
            worksheet.set_column('C:C', 20)  # Телефон
            worksheet.set_column('D:D', 25)  # Email
            worksheet.set_column('E:E', 40)  # URL
            worksheet.set_column('F:F', 20)  # Дата добавления

        print(f"Данные успешно сохранены. Добавлено {len(new_df)} новых записей.")

    except Exception as e:
        print(f"Ошибка при сохранении: {str(e)}")


def job():
    """Задача для планировщика с промежуточным сохранением данных"""
    print(f"\n=== Запуск парсера {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

    # Создаем временный файл для промежуточных результатов
    temp_file = "temp_companies_data.xlsx"
    final_file = OUTPUT_FILE

    driver = setup_driver()
    all_data = []
    processed_count = 0

    try:
        # Загружаем уже обработанные данные (если есть)
        if os.path.exists(temp_file):
            existing_data = load_existing_data(temp_file)
            if not existing_data.empty:
                all_data = existing_data.to_dict('records')
                print(f"Загружено {len(all_data)} ранее сохраненных записей")

        company_links = get_all_company_links(driver)
        print(f"Найдено {len(company_links)} компаний для обработки")

        for i, link in enumerate(company_links, 1):
            print(f"Обработка компании {i}/{len(company_links)}: {link}")
            data = parse_company_page(driver, link)
            if data:
                all_data.append({
                    'ИНН': data[0],
                    'Дата регистрации': data[1],
                    'Телефон': data[2],
                    'Email': data[3],
                    'URL': data[4],
                    'Дата добавления': data[5]
                })
                processed_count += 1

            # Промежуточное сохранение каждые 20 компаний
            if i % 20 == 0:
                print(f"\nПромежуточное сохранение после {i} компаний...")
                save_to_excel(all_data, temp_file)
                print(f"Всего сохранено записей: {len(all_data)}")

            time.sleep(3)

        # Финальное сохранение
        print("\nФинальное сохранение результатов...")
        save_to_excel(all_data, final_file)

        # Удаляем временный файл после успешного завершения
        if os.path.exists(temp_file):
            os.remove(temp_file)

        print(f"\n=== Итоги ===")
        print(f"Обработано компаний: {len(company_links)}")
        print(f"Добавлено новых записей: {processed_count}")
        print(f"Всего записей в файле: {len(all_data)}")

    except Exception as e:
        print(f"\nКритическая ошибка: {str(e)}")
        # Сохраняем прогресс в временный файл при ошибке
        if all_data:
            print("Сохранение промежуточных данных...")
            save_to_excel(all_data, temp_file)
    finally:
        driver.quit()
        print("=== Завершение работы ===")


def run_scheduler():
    """Запуск планировщика с обработкой прерываний"""
    job()  # Запуск при старте

    schedule.every().day.at("00:05").do(job)

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("Планировщик остановлен")



if __name__ == "__main__":
    run_scheduler()

