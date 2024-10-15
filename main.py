import asyncio
import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.future import select
import os
from datetime import datetime
from aioconsole import ainput
import pandas as pd
import openpyxl

# ///конфиг///
data_acquisition_interval = 10  # интервал получения данных в секундах
name_bd = 'data_weather'
name_table = 'data_weather'
export_path = 'export'
coordinates = {'lat': 55.698538,
               'lon': 37.359576}

# создать папку экспорта еcли её нет
if not os.path.exists(export_path):
    os.makedirs(export_path)

# Создаем базу данных и ORM модель для хранения данных о погоде
Base = declarative_base()
dist_bmo = {
    '51': 'Морось: легкая',
    '53': 'Морось: умеренная',
    '55': 'Морось: интенсивная',
    '56': 'Моросящий дождь: легкая',
    '57': 'Моросящий дождь: плотная интенсивность',
    '61': 'Дождь: небольшой',
    '63': 'Дождь: умеренной интенсивности',
    '65': 'Дождь: сильной интенсивности',
    '66': 'Ледяной дождь: небольшой',
    '67': 'Ледяной дождь: сильной интенсивности',
    '71': 'Снегопад: слабой интенсивности',
    '73': 'Снегопад: умеренной интенсивности',
    '75': 'Снегопад: сильной интенсивности',
    '77': 'Снежные зерна',
    '80': 'Ливни: слабые',
    '81': 'Ливни: умеренные',
    '82': 'Ливни: сильные',
    '85': 'Небольшой снегопад',
    '86': 'Сильный снегопад',
    '95': 'Гроза: слабая',
    '96': 'Гроза с небольшим градом',
    '99': 'Гроза с сильным градом'
}  # коды погоды и названия


# создать модель для базы данных
class WeatherData(Base):
    __tablename__ = name_bd

    id = Column(Integer, primary_key=True)
    timestamp = Column(String)
    temperature = Column(Float)
    wind_speed = Column(Float)
    wind_direction = Column(String)
    pressure = Column(Float)
    precipitation_type = Column(String)
    precipitation_amount = Column(Float)


# Создаем движок и сессию для асинхронной работы с базой данных
DATABASE_URL = f"sqlite+aiosqlite:///./{name_table}.db"

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


# Функция для запроса данных о погоде через API Open-Meteo
async def fetch_weather_data(session, latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": ["temperature_2m", "precipitation", "rain", "showers", "snowfall", "weather_code", "pressure_msl",
                    "wind_speed_10m", "wind_direction_10m"],

        "timezone": "Europe/Moscow",
        "wind_speed_unit": "ms",

    }
    async with session.get(url, params=params) as response:
        if response.status == 200:
            data = await response.json()

            return data['current']
        else:
            # print(f"Ошибка при запросе данных: {response.status}")
            return None


# Функция для преобразования направления ветра
def wind_direction(degrees):
    directions = ['С', 'ССВ', 'СВ', 'ВСВ', 'В', 'ВЮВ', 'ЮВ', 'ЮЮВ', 'Ю', 'ЮЮЗ', 'ЮЗ', 'ЗЮЗ', 'З', 'ЗСЗ', 'СЗ', 'ССЗ']
    index = round(degrees / 22.5) % 16
    return directions[index]


# Функция для записи данных в базу данных
async def save_weather_data(weather_data):
    async with async_session() as session:
        async with session.begin():
            new_data = WeatherData(
                timestamp=weather_data['time'].replace('T', ' '),
                temperature=weather_data["temperature_2m"],
                wind_speed=weather_data["wind_speed_10m"],
                wind_direction=wind_direction(weather_data["wind_direction_10m"]),
                pressure=round(weather_data["pressure_msl"] / 1.3332, 2),
                precipitation_type=dist_bmo.get(str(weather_data['weather_code']), "Осадков нет"),
                precipitation_amount=weather_data["precipitation"]
            )
            session.add(new_data)


# функция постояного получения и сохранения данных
async def collect_weather_data(interval: int = 180):
    async with aiohttp.ClientSession() as session:

        while True:
            weather_data = await fetch_weather_data(session, latitude=coordinates['lat'], longitude=coordinates['lon'])
            if weather_data:
                await save_weather_data(weather_data)
                # print("Данные о погоде сохранены.")
            else:
                # print("Не удалось получить данные о погоде.")
                pass

            await asyncio.sleep(interval)


# функция обработки команд
async def command_handler():
    while True:
        command = await ainput("для экспорта данных введите 'export_xlsx', для выхода введите 'exit' >>> ")
        if command == "export_xlsx":
            await export_to_xlsx()
        elif command == "exit":
            quit()
        else:
            print("Неизвестная команда. Пожалуйста, используйте 'export_xlsx'.")


# Функция для экспорта последних 10 записей в XLSX
async def export_to_xlsx():
    async with async_session() as session:
        async with session.begin():
            result = await session.execute(select(WeatherData).order_by(WeatherData.id.desc()).limit(10))
            records = result.scalars().all()

            # Преобразуем записи в DataFrame
            df = pd.DataFrame([{
                'Время': record.timestamp,
                'Температура (°C)': record.temperature,
                'Скорость ветра (м/с)': record.wind_speed,
                'Направление ветра': record.wind_direction,
                'Количество осадков (мм)': record.precipitation_amount,
                'Тип осадков': record.precipitation_type,
                'Давление (мм рт.ст.)': record.pressure,
            } for record in records])

            # Сохраняем DataFrame в файл XLSX
            excel_filename = f"{export_path}/данные погоды от {datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.xlsx"
            df.to_excel(excel_filename, index=False)

            # Открываем файл для изменения ширины столбцов
            workbook = openpyxl.load_workbook(excel_filename)
            sheet = workbook.active

            # Установка ширины столбцов
            column_widths = {
                'A': 20,  # Время
                'B': 20,  # Температура
                'C': 25,  # Скорость ветра
                'D': 20,  # Направление ветра
                'E': 20,  # Давление
                'F': 25,  # Тип осадков
                'G': 30,  # Количество осадков
            }

            for col, width in column_widths.items():
                sheet.column_dimensions[col].width = width

            # Сохраняем изменения в файл
            workbook.save(excel_filename)
            print(f"Данные успешно экспортированы в {excel_filename}.")
            await asyncio.sleep(3)


# Главная функция
async def main():
    # Создаем таблицу, если она еще не создана
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Создаем и запускаем задачи для сбора данных и обработки команд
    weather_task = asyncio.create_task(collect_weather_data(data_acquisition_interval))
    command_task = asyncio.create_task(command_handler())

    # Ждем завершения задач (не завершатся, так как это бесконечные циклы)
    await asyncio.gather(weather_task, command_task)


# Запуск асинхронной программы
if __name__ == "__main__":
    asyncio.run(main())
