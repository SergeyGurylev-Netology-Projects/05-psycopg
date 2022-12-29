import sys
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class ClientDB:
    def __init__(self, user, password, name_db):
        self.user = user
        self.password = password
        self.connection = None
        self.name_db = name_db

        if not self._connect():
            return
        if not self._create_db():
            return
        if not self._create_tables():
            return

    def _connect(self, name_db=''):
        result = True
        try:
            self.connection = psycopg2.connect(user=self.user, password=self.password, database=name_db)
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        except (Exception, Error) as error:
            print('Connection error', error)
            result = False

        return result

    def _create_db(self):
        result = True
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f'CREATE DATABASE {self.name_db}')
        except psycopg2.errors.lookup("42P04"): # Duplicate database
            pass
        except (Exception, Error) as error:
            print('Create database error', error)
            result = False

        if result:
            self.connection.close()
            result = self._connect(self.name_db)

        return result

    def _create_tables(self):
        result = True
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS client(
                        client_id SERIAL PRIMARY KEY,
                        name VARCHAR(40) NOT NULL,
                        surname VARCHAR(40),
                        email VARCHAR(40) NOT NULL UNIQUE
                    )
                    """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS phone(
                        phone_id SERIAL PRIMARY KEY,
                        number VARCHAR(15) NOT NULL UNIQUE,
                        client_id INTEGER NOT NULL REFERENCES client(client_id)
                    );
                    """)
                self.connection.commit()
        except (Exception, Error) as error:
            print('Create tables error', error)
            result = False

        return result

    def add_client(self, name: str, surname, email: str, phones: list = []):
        with self.connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO client(name, surname, email) VALUES(%s, %s, %s) RETURNING client_id
                """, (name, surname, email))
            client_id = cursor.fetchone()[0]

        for phone in phones:
            self.add_phone(client_id, phone)

    def add_phone(self, client_id, phone):
        with self.connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO phone(client_id, number) VALUES(%s, %s)
                """, (client_id, phone))
            self.connection.commit()

    def update_client(self, client_id, update_info: dict):
        if len(update_info) == 0:
            return

        with self.connection.cursor() as cursor:
            cursor.execute(f"""
                UPDATE client
                SET {' = %s, '.join(v for v in update_info.keys()) + ' = %s'}
                WHERE client_id = %s
                """, tuple([v for v in update_info.values()]) + (client_id,))
            self.connection.commit()

    def del_phone(self, client_id, phone: str):
        with self.connection.cursor() as cursor:
            cursor.execute(f"""
                DELETE FROM phone
                WHERE client_id = %s AND number = %s
                """, (client_id, phone))
            self.connection.commit()

    def del_client(self, client_id):
        with self.connection.cursor() as cursor:
            cursor.execute(f"""
                DELETE FROM phone
                WHERE client_id = %s
                """, (client_id,))
            cursor.execute(f"""
                DELETE FROM client
                WHERE client_id = %s
                """, (client_id,))
            self.connection.commit()

    def find_client(self, find_info: str):
        with self.connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT client_id
                FROM client
                WHERE name LIKE %s OR surname LIKE %s OR email LIKE %s
                
                UNION
                
                SELECT client_id
                FROM phone
                WHERE number LIKE %s
                """, (f'%{find_info}%',)*4)

            query_result = cursor.fetchall()
            return [id[0] for id in query_result]


if __name__ == '__main__':

    # 1. Функция, создающая структуру БД(таблицы)
    # Функции по созданию БД, подключени к ней и созданию таблиц вызываются в конструкторе класса
    client_db = ClientDB('postgres', '0000', 'client_db')
    if not client_db.connection:
        sys.exit(1)

    # Примеры использования функций

    # 2. Функция, позволяющая добавить нового клиента
    client_db.add_client('Иван', 'Иванов', 'i_ivanov@servername.ru', ['0000000000', '1111111111', '3333333333']) # 18
    client_db.add_client('Петр', 'Петров', 'p_petrov@servername.ru', ['2222222222'])
    client_db.add_client('Максим', 'Кузьмин', 'm.kuzmin@servername.ru') # 21
    client_db.add_client('Григорий', 'Гришин', 'g_grishin@servername.ru', ['4444444444', '5555555555']) # 22

    # 3. Функция, позволяющая добавить телефон для существующего клиента
    client_db.add_phone(21, '1234567890')

    # 4. Функция, позволяющая изменить данные о клиенте
    client_db.update_client(21, {'email': 'm_kuzmin@servername.ru'})
    client_db.update_client(22, {'name': 'Александр', 'surname': 'Васильев','email': 'a_vasilev@servername.ru'})

    # 5. Функция, позволяющая удалить телефон для существующего клиента
    client_db.del_phone(18, '0000000000')

    # 6. Функция, позволяющая удалить существующего клиента
    client_db.del_client(18)

    # 7. Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
    client_ids = client_db.find_client('Петров')
    print(client_ids)

    client_ids = client_db.find_client('servername.ru')
    print(client_ids)

    client_ids = client_db.find_client('00')
    print(client_ids)


    client_db.connection.close()
    print("Connection closed successfully")
