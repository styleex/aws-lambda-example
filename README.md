# Пример lambda функции с api gateway

Пример aws lambda фукнкции на python:
1. Имеет авторизацию
2. Работает с RabbitMQ
3. Работает с базой
4. Логирует ошибки
5. Проинтегрирована с aws api gateway

## Переменные окружения для запуска:

1. AMQP_URL - URI подключения к rabbitmq, пример:

    amqps://login:pass@domain.ru:5671


2. POSTGRESQL_DB_URL - URI подключения к постгресу (таблица будет создана автомтатом), пример:

    postgresql://login:pass@domain.ru/mydb?connect_timeout=10&application_name=myapp
