CREATE DATABASE WeatherDB;
USE WeatherDB;

CREATE TABLE Country
(
    id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
    country_name VARCHAR(30) NOT NULL UNIQUE,
    latitude DOUBLE(7, 2) NOT NULL,
    longitude DOUBLE(7, 2) NOT NULL
);

CREATE TABLE City
(
    id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
    country_id INTEGER NOT NULL,
    city_name VARCHAR(30) NOT NULL,
    latitude DOUBLE(7, 2) NOT NULL,
    longitude DOUBLE(7, 2) NOT NULL,
    CONSTRAINT fk_country_id FOREIGN KEY (country_id) REFERENCES Country(id),
    CONSTRAINT uk_city UNIQUE(country_id, city_name)
);

CREATE TABLE Temperature
(
    id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
    value DOUBLE(7, 2),
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    city_id INTEGER NOT NULL,
    CONSTRAINT fk_city_id FOREIGN KEY (city_id) REFERENCES City(id),
    CONSTRAINT uk_temp UNIQUE(city_id, timestamp)
);
