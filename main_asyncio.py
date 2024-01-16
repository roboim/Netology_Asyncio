import asyncio
import datetime

import aiohttp
from more_itertools import chunked

from models import init_db, Session, SwapiPeople

MAX_CHUNK = 5  # Количество запросов в серии


async def get_person(person_id: int, session: aiohttp.ClientSession) -> aiohttp.client_reqrep.ClientResponse.json:
    """Получить данные из API сервиса"""
    http_response = await session.get(f"https://swapi.dev/api/people/{person_id}/")
    #  https://swapi.py4e.com/api/people/{person_id}/ - работает быстрее
    json_data = await http_response.json()
    # print(json_data)
    return json_data


async def insert_records(records: tuple, id_s: list):
    """Выгрузка записей в базу данных"""
    required_fields = ['birth_year', 'eye_color', 'films', 'gender', 'hair_color', 'height', 'homeworld', 'mass',
                       'name', 'skin_color', 'species', 'starships', 'vehicles']
    prepared_data = []
    for number, record in enumerate(records):
        person_id = int(id_s[0]) + number
        try:
            if record['detail'] == 'Not found':
                print(f'Запись отсутствует для данного id: {person_id}')
                pass
        except KeyError:
            try:
                person_data = dict()
                person_data['id'] = person_id
                for field in required_fields:
                    person_data[field] = record[field]
                # print(person_data)
                prepared_data.append(person_data)
            except KeyError:
                print(f'Поле {field} отсутствует')

    persons = [SwapiPeople(json=record) for record in prepared_data]
    async with Session() as session:
        session.add_all(persons)
        await session.commit()


async def api_processing():
    """Загрузка данных по API и выгрузка их в локальную базу данных"""
    await init_db()  # Инициализируем БД
    session = aiohttp.ClientSession()  # Открываем сессию
    for people_id_chunk in chunked(range(1, 86), MAX_CHUNK):  # Создаем группы запросов для асинхронного вызова
        coro_func = [get_person(person_id, session) for person_id in people_id_chunk]  # Создаём корофункции
        result = await asyncio.gather(*coro_func)  # Исполняем
        asyncio.create_task(insert_records(result, people_id_chunk))  # Добавляем новые асинхронные задачи

    await session.close()  # Закрываем сессию
    all_tasks_set = asyncio.all_tasks() - {asyncio.current_task()}  # Составляем множество задач, кроме api_processing
    await asyncio.gather(*all_tasks_set)  # Ожидаем исполнения всех задач, кроме api_processing


if __name__ == '__main__':
    start = datetime.datetime.now()
    print(f'Начало выгрузки данных по API: {start}')
    asyncio.run(api_processing())  # Запуск асинхронных задач
    finish = datetime.datetime.now()
    print(f'Завершение загрузки данных в локальную базу данных: {finish}')
    print(f'Время выполнения задачи: {finish - start}')
