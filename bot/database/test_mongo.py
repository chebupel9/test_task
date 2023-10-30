import pymongo

# Адрес сервера MongoDB
mongo_url = "mongodb://test:password@80.87.197.159:27017/"

# Устанавливаем соединение с MongoDB
client = pymongo.MongoClient(mongo_url)

# Получаем список всех баз данных на сервере
database_names = client.list_database_names()

# Выводим список баз данных
for db_name in database_names:
    print(db_name)
