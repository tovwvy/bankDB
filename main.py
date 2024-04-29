import psycopg2
import random
from datetime import datetime, timedelta

def connect_to_database():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="BankDB",
            user="Roman",
            password="123",
            host="localhost",
            port="5432"
        )
        print("Підключення до бази даних успішне!")
    except psycopg2.OperationalError as e:
        print(f"Помилка при підключенні до бази даних: {e}")
    return conn

def create_tables(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kasy (
                kasa_id SERIAL PRIMARY KEY,
                klienti_obslugovani INTEGER DEFAULT 0,
                chas_vitrateny INTEGER DEFAULT 0
            );
        """)

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
        print("Таблиці успішно створені!")
    except psycopg2.Error as e:
        print(f"Помилка при створенні таблиць: {e}")
    finally:
        cursor.close()

def populate_tables(conn):
    cursor = conn.cursor()
    try:
        # Вставка даних у таблицю kasy
        for _ in range(5):
            cursor.execute("INSERT INTO kasy (klienti_obslugovani, chas_vitrateny) VALUES (%s, %s);", (random.randint(0, 10), random.randint(0, 1000)))

        # Вставка даних у таблицю klienti
        for _ in range(20):
            cursor.execute("INSERT INTO klienti (chas_prihodu, cherga_id) VALUES (%s, %s);", (datetime.now() - timedelta(days=random.randint(0, 30)), random.randint(1, 5)))

        conn.commit()
        print("Дані успішно додані до таблиць!")
    except psycopg2.Error as e:
        print(f"Помилка при додаванні даних до таблиць: {e}")
    finally:
        cursor.close()

def simulate_service(conn):
    cursor = conn.cursor()
    try:
        NUM_KASY = 5
        MEAN_ARRIVAL_TIME = 1
        MEAN_SERVICE_TIME = 4.5
        WORK_DAY_MINUTES = 8 * 60

        create_tables(conn)

        kasa_ids = []
        for _ in range(NUM_KASY):
            cursor.execute("INSERT INTO kasy DEFAULT VALUES RETURNING kasa_id;")
            kasa_id = cursor.fetchone()[0]
            kasa_ids.append(kasa_id)

        current_time = datetime.now()
        while (datetime.now() - current_time).total_seconds() / 60 < WORK_DAY_MINUTES:
            for kasa_id in kasa_ids:
                if random.random() < 1 / (MEAN_ARRIVAL_TIME * NUM_KASY):
                    cursor.execute("INSERT INTO klienti (chas_prihodu, cherga_id) VALUES (%s, %s);", (datetime.now(), kasa_id))

            for kasa_id in kasa_ids:
                cursor.execute("SELECT klient_id, chas_prihodu FROM klienti WHERE cherga_id = %s AND chas_obslugovuvannya IS NULL ORDER BY chas_prihodu;", (kasa_id,))
                result = cursor.fetchone()
                if result is not None:
                    klient_id, chas_prihodu = result
                    chas_obslugovuvannya = chas_prihodu + timedelta(minutes=random.expovariate(1 / MEAN_SERVICE_TIME))
                    cursor.execute("UPDATE klienti SET chas_obslugovuvannya = %s WHERE klient_id = %s;", (chas_obslugovuvannya, klient_id))
                    cursor.execute("UPDATE kasy SET klienti_obslugovani = klienti_obslugovani + 1, chas_vitrateny = chas_vitrateny + EXTRACT(EPOCH FROM (%s - %s)) WHERE kasa_id = %s;", (chas_obslugovuvannya, chas_prihodu, kasa_id))

            conn.commit()
        print("Симуляція обслуговування завершена!")
    except psycopg2.Error as e:
        print(f"Помилка при симуляції обслуговування: {e}")
    finally:
        cursor.close()

def get_statistics(conn):
    cursor = conn.cursor()
    try:
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

        cursor.execute("""
            SELECT kasa_id, klienti_obslugovani FROM kasy ORDER BY klienti_obslugovani DESC LIMIT 1;
        """)
        result = cursor.fetchone()
        print(f"Найбільш експлуатована каса: Каса {result[0]} з {result[1]} клієнтами")

        cursor.execute("""
            SELECT EXTRACT(HOUR FROM chas_prihodu) AS hour, COUNT(*) AS total_clients
            FROM klienti
            GROUP BY hour
            ORDER BY total_clients DESC
            LIMIT 1;
        """)
        result = cursor.fetchone()
        print(f"Найбільша кількість клієнтів була о {int(result[0])} годині: {result[1]} клієнтів")
    except psycopg2.Error as e:
        print(f"Помилка при отриманні статистики: {e}")
    finally:
        cursor.close()

def main():
    conn = connect_to_database()
    if conn is not None:
        create_tables(conn)
        populate_tables(conn)  # Додано заповнення даних
        simulate_service(conn)
        get_statistics(conn)
        conn.close()


if __name__ == "__main__":
    main()
