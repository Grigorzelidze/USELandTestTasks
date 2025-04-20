# Данный python-скрипт имитирует запрос к БД
# Напишите ваш SQL-запрос в query и запустите данный python-скрипт для получения результата
# Перед запуском скрипта установите библиотеку duckdb

# Установка библиотеки duckdb
# pip install duckdb duckdb-engine

# Импорт библиотек
import pandas as pd
import numpy as np
import duckdb

# Задание таблиц БД
users = pd.read_csv('users.csv')
course_users = pd.read_csv('course_users.csv')
courses = pd.read_csv('courses.csv')
course_types = pd.read_csv('course_types.csv')
lessons = pd.read_csv('lessons.csv')
subjects = pd.read_csv('subjects.csv')
cities = pd.read_csv('cities.csv')
homework_done = pd.read_csv('homework_done.csv')
homework = pd.read_csv('homework.csv')
homework_lessons = pd.read_csv('homework_lessons.csv')
user_roles = pd.read_csv('user_roles.csv') 
print("\n\n")

# Задание SQL-запроса
query = """
SELECT 
    c.id AS course_id,
    c.name AS course_name,
    s.name AS subject_name,
    s.project AS subject_type,
    ct.name AS course_type,
    c.starts_at::date AS course_start_date,
    cu.user_id AS student_id,
    u.last_name AS student_last_name,
    ci.name AS student_city,
    cu.active AS is_student_active,
    cu.created_at::date AS course_open_date,
    FLOOR(cu.available_lessons / c.lessons_in_month)::int4 AS months_course_open,
    coalesce(h.cnt, 0) AS completed_homework_count
FROM
    courses c
JOIN 
    subjects s ON c.subject_id = s.id
JOIN 
    course_types ct ON c.course_type_id = ct.id
    	AND ct.id IN (1, 6) --годовые курсы (не нашёл атрибута для разделения на ЕГЭ, ОГЭ и др.)
JOIN 
    course_users cu ON c.id = cu.course_id
JOIN 
    users u ON cu.user_id = u.id
        and u.user_role_id = 5 --отбираем только студентов
LEFT JOIN 
    cities ci ON u.city_id = ci.id
LEFT JOIN
	(
	SELECT 
		hd.user_id
		,l.course_id
		,count(DISTINCT hd.homework_id) cnt
	FROM homework_done hd
		JOIN homework_lessons hl ON hl.homework_id = hd.homework_id
		JOIN lessons l ON l.id = hl.lesson_id
	GROUP BY 1, 2
	) h ON h.user_id = u.id AND h.course_id = c.id -- в этом подзапросе получаем количество уникальных сданных ДЗ, группируя по пользователю и курсу
ORDER BY 
    completed_homework_count, c.id, cu.user_id
"""

# Выполнение SQL-запроса
df_result = duckdb.query(query).to_df()
df_result.to_csv('zad1.csv', index=False)
# Вывод результата
print(df_result)
# Загрузка данных (предположим, что данные выгружены в CSV-файл)
df = pd.read_csv('zad1.csv')

# 1. Проверка на дубликаты
print("Количество дубликатов:", df.duplicated().sum())
df = df.drop_duplicates()  # Удаление дубликатов

# 2. Проверка на пропуски
print("Пропуски в данных:")
print(df.isnull().sum())

# Если есть пропуски, решаем, как их обработать:
# - Для числовых данных можно заполнить средним/медианой или удалить строки.
# - Для категориальных данных можно заполнить модой или удалить строки.
# Пример обработки пропусков:
df['course_start_date'] = df['course_start_date'].fillna(method='ffill')  # Заполнение пропусков для дат
df['student_city'] = df['student_city'].fillna('Неизвестно')  # Заполнение пропусков для города
df = df.dropna(subset=['student_id', 'course_open_date'])  # Удаление строк с критическими пропусками

# 3. Проверка типов данных
print("Типы данных:")
print(df.dtypes)

# Преобразование типов данных, если необходимо
df['course_start_date'] = pd.to_datetime(df['course_start_date'])  # Преобразование в datetime
df['course_open_date'] = pd.to_datetime(df['course_open_date'])  # Преобразование в datetime

# 4. Проверка на аномальные значения
# Пример: проверка на отрицательные значения в числовых столбцах
numeric_columns = ['months_course_open', 'completed_homework_count']
for col in numeric_columns:
    if (df[col] < 0).any():
        print(f"Аномальные значения в столбце {col}:")
        print(df[df[col] < 0])

# Обработка аномальных значений
df = df[df['months_course_open'] >= 0]  # Удаление строк с отрицательными значениями
df = df[df['completed_homework_count'] >= 0]

# Сохранение предобработанного датасета
df.to_csv('preprocessed_dataset.csv', index=False)
# Загрузка предобработанного датасета
df = pd.read_csv('preprocessed_dataset.csv')

# Преобразование дат в datetime, если они еще не преобразованы
df['course_start_date'] = pd.to_datetime(df['course_start_date'])
df['course_open_date'] = pd.to_datetime(df['course_open_date'])

# Вычисление разницы между датой старта курса и датой присоединения ученика
df['days_since_start'] = (df['course_open_date'] - df['course_start_date']).dt.days

# Определение волн
def assign_wave(days):
    if days <= 0:
        return 0  # 0 волна
    elif 1 <= days <= 7:
        return 1  # 1 волна
    elif 8 <= days <= 14:
        return 2  # 2 волна
    elif 15 <= days <= 21:
        return 3  # 3 волна
    elif 22 <= days <= 28:
        return 4  # 4 волна
    else:
        return 5  # 5 волна

# Применение функции для определения волны
df['wave'] = df['days_since_start'].apply(assign_wave)

# Сохранение результата
df.to_csv('students_with_waves.csv', index=False)

# Вывод статистики по волнам
wave_stats = df['wave'].value_counts().sort_index()
print("Статистика по волнам:")
print(wave_stats)