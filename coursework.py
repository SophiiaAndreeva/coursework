import os
import asyncio
import datetime
import logging
from typing import Optional
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCOPES = ['https://www.googleapis.com/auth/calendar'] # Список разрешений, который будет запрошен у пользователя
POLLING_LOOKAHEAD_MINUTES = 15  # Интервал поиска событий
POLLING_SLEEP_SECONDS = 60  # Пауза между циклами опроса
PROCESSED_IDS = set() # Множество для фильтрации дубликатов
_LAST_CLEANUP_DATE = None # Переменная для хранения даты последней очистки множества


def _cleanup_processed_ids():
    """
    Очищает множество PROCESSED_IDS.
    Выполняется один раз в день.
    """
    global _LAST_CLEANUP_DATE
    today = datetime.datetime.now(datetime.UTC).date()

    if _LAST_CLEANUP_DATE != today:
        old_count = len(PROCESSED_IDS)
        PROCESSED_IDS.clear()
        _LAST_CLEANUP_DATE = today
        logging.info(f"Очистка выполнена. Удалено {old_count} событий.")


def get_calendar_service():
    """Авторизация и получение объекта для работы с Google Calendar API."""
    creds = None

    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('calendar', 'v3', credentials=creds)


def create_calendar_event(service, user_id: int, title: str, start_time: str, end_time: str) -> Optional [str]:
    """Создает событие в Google Календаре."""
    try:
        event_body = {
            'summary': title,
            'description': f"tg_user_id: {user_id}",
            'start': {'dateTime': start_time, 'timeZone': 'UTC'},
            'end': {'dateTime': end_time, 'timeZone': 'UTC'},
        }
        event = service.events().insert(calendarId='primary', body=event_body).execute()
        logging.info(f"Событие создано. ID: {event.get('id')}")
        return event.get('id')
    except HttpError as error:
        logging.error(f"Ошибка при создании события: {error}")
        return None


def delete_calendar_event(service, calendar_event_id: str) -> bool:
    """Удаляет одно событие по ID."""
    try:
        service.events().delete(calendarId='primary', eventId=calendar_event_id).execute()
        logging.info(f"Событие {calendar_event_id} удалено.")
        # Очистка множества после удаления события
        if calendar_event_id in PROCESSED_IDS:
            PROCESSED_IDS.discard(calendar_event_id)
        return True
    except HttpError as error:
        logging.error(f"Ошибка удаления {calendar_event_id}: {error}")
        return False


def delete_calendar_events(service, calendar_event_ids: list[str]) -> dict[str, str]:
    """Пакетное удаление событий."""
    results = {}

    def callback(request_id, _response, exception):
        if exception:
            results[request_id] = f"Error: {exception}"
            logging.error(f"Ошибка удаления {request_id}: {exception}")
        else:
            results[request_id] = "Deleted"
            logging.info(f"Задача {request_id} удалена.")
            # Очистка множества после удаления события
            PROCESSED_IDS.discard(request_id)

    batch = service.new_batch_http_request(callback=callback)
    for event_id in calendar_event_ids:
        if event_id:
            batch.add(service.events().delete(calendarId='primary', eventId=event_id), request_id=event_id)

    try:
        batch.execute()
    except HttpError as error:
        logging.error(f"Ошибка пакетного удаления: {error}")

    return results


async def poll_calendar_service(service, bot_module):
    """
    Бесконечный фоновый опрос календаря.
    """
    logging.info("Фоновый мониторинг запущен")
    while True:
        try:
            # Очистка мноджества
            _cleanup_processed_ids()

            now = datetime.datetime.now(datetime.UTC)
            time_min = now.isoformat() + 'Z'
            time_max = (now + datetime.timedelta(minutes=POLLING_LOOKAHEAD_MINUTES)).isoformat() + 'Z'

            events_result = service.events().list(
                calendarId='primary',
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy='startTime'
            ).execute()

            events = events_result.get('items', [])

            for event in events:
                event_id = event.get('id')
                description = event.get('description', '')

                # Пропуск обработанных событий
                if event_id in PROCESSED_IDS:
                    continue

                if "tg_user_id:" in description:
                    try:
                        user_id = int(description.split("tg_user_id:")[1].strip())
                        summary = event.get('summary', 'Без названия')

                        # Проверка наличия метода у bot_module
                        if bot_module and hasattr(bot_module, 'send_calendar_reminder'):
                            bot_module.send_calendar_reminder(user_id, summary)
                            PROCESSED_IDS.add(event_id)
                            logging.info(f"Отправлено напоминание пользователю {user_id}: {summary}")
                        else:
                            logging.error("bot_module не имеет метода send_calendar_reminder")

                    except (ValueError, IndexError) as e:
                        logging.warning(f"Ошибка парсинга user_id в событии {event_id}: {e}")

            await asyncio.sleep(POLLING_SLEEP_SECONDS)

        except HttpError as error:
            logging.error(f"Ошибка API при опросе: {error}")
            await asyncio.sleep(5)
        except Exception as error:
            logging.error(f"Ошибка в polling: {error}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    logging.info("Запуск модуля интеграции")

    service = get_calendar_service()

    logging.info("Авторизация успешно завершена")
    logging.info("Модуль готов к работе")