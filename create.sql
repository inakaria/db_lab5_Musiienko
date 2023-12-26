CREATE TABLE NEW_Credit_Card
(
  routing_number VARCHAR(9) NOT NULL,
  card_type VARCHAR(20) NOT NULL,
  PRIMARY KEY (routing_number)
);

CREATE TABLE NEW_Purchaser
(
  purchaser_id INT NOT NULL,
  first_name VARCHAR(30) NOT NULL,
  last_name VARCHAR(30) NOT NULL,
  country VARCHAR(30) NOT NULL,
  routing_number VARCHAR(9) NOT NULL,
  PRIMARY KEY (purchaser_id),
  FOREIGN KEY (routing_number) REFERENCES NEW_Credit_Card(routing_number)
);

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
);