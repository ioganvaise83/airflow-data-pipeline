CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dds;

-- ODS: "сырые" счета
CREATE TABLE IF NOT EXISTS ods.accounts (
    id SERIAL PRIMARY KEY,
    account_number TEXT NOT NULL,
    contractor_inn TEXT NOT NULL,
    amount NUMERIC,
    created_at TIMESTAMP DEFAULT now()
);

-- DDS: справочник контрагентов
CREATE TABLE IF NOT EXISTS dds.contractors (
    id SERIAL PRIMARY KEY,
    inn TEXT UNIQUE NOT NULL,
    name TEXT
);

-- DDS: нормализованные счета
CREATE TABLE IF NOT EXISTS dds.accounts (
    id SERIAL PRIMARY KEY,
    account_number TEXT NOT NULL,
    contractor_id INT REFERENCES dds.contractors(id),
    amount NUMERIC,
    created_at TIMESTAMP
);
-- Первичное наполнение тестовыми данными
INSERT INTO dds.contractors (inn, name)
VALUES
    ('7701000001', 'ООО Ромашка'),
    ('7702000002', 'ИП Петров'),
    ('7703000003', 'АО ТехСнаб');

INSERT INTO ods.accounts (account_number, contractor_inn, amount, created_at)
VALUES
    ('40802810000000000001', '7701000001', 150000.00, now()),
    ('40802810000000000002', '7701000001', 5000.50,  now()),
    ('40802810000000000003', '7702000002', 72500.00, now()),
    ('40802810000000000004', '7703000003', 0.00,     now()),
    ('40802810000000000005', '9999999999', 1200.00,  now()); -- контрагент отсутствует
