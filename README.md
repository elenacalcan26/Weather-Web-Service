# Weather-Web-Service

#### Calcan Elena-Claudia
#### 343C3

## Synopsys
---------------

- este implementat un server web in **Python** si **Flask** microframework
- serever-ul web gestioneaza si o baza de date ce contine informatii despre temperaturile
unor orase si tari care au anumite coordonate geografice
- server-ul suporta metodele **POST**, **GET**, **PUT** & **DELETE**

## Structura si Implementare
-----------------------------

### docker-compose.yml
  - creeaza componentele principale ale aplicatiei care sunt create in contanere
    1. **mysql**
        - este folosit pentru rularea bazei de date
        - este folosit **MySQL** pentru a modela mai usor baza de date
        - pentru persistenta datelor se foloseste volumul: **db_data:/var/lib/mysql**
        - pentru conectare se folosesc credentialele prcizate si in varibilele de mediu:

                user: admin
                password: admin

    2. **adminer**
        - utilitarul de gestiune a bazei de date
        - este accesibil pe **localhost** pe portul **8080**
    3. **meteo-server**
        - este folosit pentru rularea server-ului
        - este accesibil pe **localhost** pe portul **6000**
        - face build la director-ul **src/**
### db/init_db.sql
  - script folosit pentru initializarea bazei de date, **WeatherDB**, si a tabelelor din ea

### src/
- aici se afla toate fisierle necesare implementarii server-ului
  1. **Dockerfile**
       - buildeaza imaginea de Docker a server-ului meteo
  2. **requirements.txt**
       - contine toate dependentele server-ului care sunt instalate in container
  3. **server.py**
        - sunt implementate API-urile care in functie de cerere se face operatia
        corespunzatoare asupra bazeid de date si server-ul trimite un raspuns in urma
        operatiilor
  4. **db_utils.py**
        - fisier helper in care sunt implementate comenzi **MySQL**, folosite de server

## Rulare
-------------
        docker compose -f docker-compose.yml up