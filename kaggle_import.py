import csv
import psycopg2

username = 'postgres'
password = 'undrugcat10'
database = 'db_lab5'
host = 'localhost'
port = '5432'

INPUT_CSV_FILE = 'cars.csv'

query_01 = '''
CREATE TABLE NEW_Credit_Card
(
  routing_number VARCHAR(9) NOT NULL,
  card_type VARCHAR(20) NOT NULL,
  PRIMARY KEY (routing_number)
)
'''
query_02 = '''
CREATE TABLE NEW_Purchaser
(
  purchaser_id INT NOT NULL,
  first_name VARCHAR(30) NOT NULL,
  last_name VARCHAR(30) NOT NULL,
  country VARCHAR(30) NOT NULL,
  routing_number VARCHAR(9) NOT NULL,
  PRIMARY KEY (purchaser_id),
  FOREIGN KEY (routing_number) REFERENCES NEW_Credit_Card(routing_number)
)
'''
query_03 = '''
CREATE TABLE NEW_Car
(
  car_id VARCHAR(20) NOT NULL,
  brand VARCHAR(20) NOT NULL,
  model VARCHAR(20) NOT NULL,
  color VARCHAR(20) NOT NULL,
  year_of_manufacture INT NOT NULL,
  price INT NOT NULL,
  purchaser_id INT NOT NULL,
  PRIMARY KEY (car_id),
  FOREIGN KEY (purchaser_id) REFERENCES NEW_Purchaser(purchaser_id)
)
'''

query_1 = '''
INSERT INTO NEW_Credit_Card (routing_number, card_type) VALUES (%s, %s)
'''
query_2 = '''
INSERT INTO NEW_Purchaser (purchaser_id, first_name, last_name, country, routing_number) VALUES (%s, %s, %s, %s, %s)
'''
query_3 = '''
INSERT INTO NEW_Car (car_id, brand, model, color, year_of_manufacture, price, purchaser_id) VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

with open(INPUT_CSV_FILE, newline='', encoding='utf-8') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader)
    conn = psycopg2.connect(user=username, password=password, database=database, host=host, port=port)
    cursor = conn.cursor()
    cursor.execute(query_01)
    cursor.execute(query_02)
    cursor.execute(query_03)

    for row in csvreader:
        routing_number = row[11]
        card_type = row[7]

        cursor.execute(query_1, (routing_number, card_type))

    csvfile.seek(0)
    next(csvreader)

    for row in csvreader:
        purchaser_id = row[9]
        first_name = row[0]
        last_name = row[1]
        country = row[2]
        routing_number = row[11]
        cursor.execute(query_2, (purchaser_id, first_name, last_name, country, routing_number))

    csvfile.seek(0)
    next(csvreader)

    for row in csvreader:
        car_id = row[8]
        brand = row[3]
        model = row[4]
        color = row[5]
        year_of_manufacture = row[6]
        price = row[10]
        purchaser_id = row[9]
        cursor.execute(query_3, (car_id, brand, model, color, year_of_manufacture, price, purchaser_id))

    conn.commit()
    cursor.close()
    conn.close()
