import psycopg2
import matplotlib.pyplot as plt

username = 'postgres'
password = 'undrugcat10'
database = 'db_lab5'
host = 'localhost'
port = '5432'

query_1 = '''
CREATE VIEW EachCustomerPaid AS
SELECT CONCAT(LEFT(first_name, 1), '. ', last_name) AS customer, price
FROM new_purchaser
JOIN new_car using (purchaser_id)
GROUP BY customer, price;
'''
query_2 = '''
CREATE VIEW PercCardUsed AS
SELECT card_type, COUNT(*) as count
FROM new_credit_card
GROUP BY card_type
HAVING card_type <> 'mastercard';
'''
query_3 = '''
CREATE VIEW YearToPrice AS
SELECT year_of_manufacture, price
FROM new_car
ORDER BY year_of_manufacture;
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:

    cur = conn.cursor()

    cur.execute('DROP VIEW IF EXISTS EachCustomerPaid')
    cur.execute(query_1) # How much each customer paid
    cur.execute('SELECT * FROM EachCustomerPaid')
    customers = []
    total = []

    for row in cur:
        customers.append(row[0])
        total.append(row[1])

    x_range = range(len(customers))
 
    figure, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3)
    bar = bar_ax.bar(x_range, total, label='Total')
    bar_ax.bar_label(bar, label_type='center')
    bar_ax.set_xticks(x_range)
    bar_ax.set_xticklabels(customers)
    bar_ax.set_xlabel('Покупці')
    bar_ax.set_ylabel('Сума, $')
    bar_ax.set_title('Загальна сума, на яку покупці зробили замовлення')

    cur.execute('DROP VIEW IF EXISTS PercCardUsed')
    cur.execute(query_2) # Percentage of using each card type for payment, except mastercard
    cur.execute('SELECT * FROM PercCardUsed')
    card_type = []
    card_count = []

    for row in cur:
        card_type.append(row[0])
        card_count.append(row[1])

    pie_ax.pie(card_count, labels=card_type, autopct='%1.1f%%')
    pie_ax.set_title('Частка замовлень через кожен вид платежу, окрім mastercard')

    cur.execute('DROP VIEW IF EXISTS YearToPrice')
    cur.execute(query_3) # Dependency between year of manufacture and price of cars
    cur.execute('SELECT * FROM YearToPrice')
    year = []
    item_price = []

    for row in cur:
        year.append(row[0])
        item_price.append(row[1])

    mark_color = 'blue'
    graph_ax.plot(year, item_price, color=mark_color, marker='o')

    for qnt, price in zip(year, item_price):
        graph_ax.annotate(price, xy=(qnt, price), color=mark_color,
                          xytext=(7, 2), textcoords='offset points')    

    graph_ax.set_xlabel('Рік')
    graph_ax.set_ylabel('Ціна')
    graph_ax.set_title('Графік залежності ціни від року виготовлення автомобіля')


mng = plt.get_current_fig_manager()
mng.resize(1400, 600)

plt.show()
