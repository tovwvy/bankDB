import psycopg2
import random
from datetime import datetime, timedelta

# Функція для створення таблиць у базі даних
def create_tables():
    conn = psycopg2.connect(
        dbname="BankDB",
        user="roman",
        password="123",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    
    # Створення таблиці kasy
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kasy (
            kasa_id SERIAL PRIMARY KEY,
            klienti_obslugovani INTEGER DEFAULT 0,
            chas_vitrateny INTEGER DEFAULT 0
        );
    """)

    # Створення таблиці klienti
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS klienti (
            klient_id SERIAL PRIMARY KEY,
            chas_prihodu TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            chas_obslugovuvannya TIMESTAMP,
            cherga_id INTEGER,
            FOREIGN KEY (cherga_id) REFERENCES kasy(kasa_id)
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()

# Функція для симуляції обслуговування клієнтів та збереження даних у базі
def simulate_service():
    conn = psycopg2.connect(
        dbname="your_database_name",
        user="your_username",
        password="your_password",
        host="your_host",
        port="your_port"
    )
    cursor = conn.cursor()

    # Кількість кас
    NUM_KASY = 5
    # Середній час між надходженням клієнтів (в хвилинах)
    MEAN_ARRIVAL_TIME = 1
    # Середній час обслуговування клієнтів (в хвилинах)
    MEAN_SERVICE_TIME = 4.5
    # Робочий день (в хвилинах)
    WORK_DAY_MINUTES = 8 * 60

    # Створення кас
    create_tables()

    # Симуляція обслуговування клієнтів
    current_time = datetime.now()
    while (datetime.now() - current_time).total_seconds() / 60 < WORK_DAY_MINUTES:
        # Надходження нового клієнта
        for kasa_id in range(1, NUM_KASY + 1):
            if random.random() < 1 / (MEAN_ARRIVAL_TIME * NUM_KASY):
                cursor.execute("INSERT INTO klienti (chas_prihodu, cherga_id) VALUES (%s, %s);", (datetime.now(), kasa_id))
        # Обслуговування клієнтів
        for kasa_id in range(1, NUM_KASY + 1):
            cursor.execute("SELECT klient_id, chas_prihodu FROM klienti WHERE cherga_id = %s AND chas_obslugovuvannya IS NULL ORDER BY chas_prihodu;", (kasa_id,))
            result = cursor.fetchone()
            if result is not None:
                klient_id, chas_prihodu = result
                chas_obslugovuvannya = chas_prihodu + timedelta(minutes=random.expovariate(1 / MEAN_SERVICE_TIME))
                cursor.execute("UPDATE klienti SET chas_obslugovuvannya = %s WHERE klient_id = %s;", (chas_obslugovuvannya, klient_id))
                cursor.execute("UPDATE kasy SET klienti_obslugovani = klienti_obslugovani + 1, chas_vitrateny = chas_vitrateny + EXTRACT(EPOCH FROM (%s - %s)) WHERE kasa_id = %s;", (chas_obslugovuvannya, chas_prihodu, kasa_id))
        conn.commit()

    cursor.close()
    conn.close()

# Функція для виконання SQL-запитів та виведення статистики
def get_statistics():
    conn = psycopg2.connect(
        dbname="your_database_name",
        user="your_username",
        password="your_password",
        host="your_host",
        port="your_port"
    )
    cursor = conn.cursor()

    # Запит на статистику
    cursor.execute("""
        SELECT 
            COUNT(*) AS total_clients,
            SUM(EXTRACT(EPOCH FROM (chas_obslugovuvannya - chas_prihodu))) / 60 AS total_service_time_minutes,
            AVG(EXTRACT(EPOCH FROM (chas_obslugovuvannya - chas_prihodu))) / 60 AS average_service_time_minutes,
            MIN(EXTRACT(EPOCH FROM (chas_obslugovuvannya - chas_prihodu))) / 60 AS min_service_time_minutes,
            MAX(EXTRACT(EPOCH FROM (chas_obslugovuvannya - chas_prihodu))) / 60 AS max_service_time_minutes
        FROM klienti;
    """)
    result = cursor.fetchone()
    print("Статистика за робочий день:")
    print(f"Кількість клієнтів: {result[0]}")
    print(f"Загальний час обслуговування (хв): {result[1]}")
    print(f"Середній час обслуговування (хв): {result[2]}")
    print(f"Мінімальний час обслуговування (хв): {result[3]}")
    print(f"Максимальний час обслуговування (хв): {result[4]}")

    # Запит для знаходження найбільш експлуатованої каси
    cursor.execute("""
        SELECT kasa_id, klienti_obslugovani FROM kasy ORDER BY klienti_obslugovani DESC LIMIT 1;
    """)
    result = cursor.fetchone()
    print(f"Найбільш експлуатована каса: Каса {result[0]} з {result[1]} клієнтами")

    # Запит для знаходження години з найбільшою кількістю клієнтів
    cursor.execute("""
        SELECT EXTRACT(HOUR FROM chas_prihodu) AS hour, COUNT(*) AS total_clients
        FROM klienti
        GROUP BY hour
        ORDER BY total_clients DESC
        LIMIT 1;
    """)
    result = cursor.fetchone()
    print(f"Найбільша кількість клієнтів була о {int(result[0])} годині: {result[1]} клієнтів")

    cursor.close()
    conn.close()

# Основна функція
def main():
    create_tables()
    simulate_service()
    get_statistics()

if __name__ == "__main__":
    main()
