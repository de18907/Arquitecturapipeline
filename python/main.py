import yfinance as yf
import pandas as pd
import psycopg2
from datetime import datetime

# Conexión a PostgreSQL
connection=psycopg2.connect(
    host="localhost",
    user="sminaya",
    password="dDihi564",
    database="LandingZone",
    port="5432"
)

cursor = connection.cursor()

# 1. Crear tablas si no existen

# Tabla para la información básica de cada banco
create_basic_info = """
CREATE TABLE IF NOT EXISTS Informaciones_Basicas (
    ticker VARCHAR PRIMARY KEY,
    industry TEXT,
    sector TEXT,
    full_time_employees INTEGER,
    city TEXT,
    phone TEXT,
    state TEXT,
    country TEXT,
    website TEXT,
    address TEXT
);
"""
# Tabla para el precio diario en bolsa
create_daily_prices = """
CREATE TABLE IF NOT EXISTS Precio_Diario_Bolsa (
    ticker VARCHAR,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    PRIMARY KEY (ticker, date)
);
"""

# Tabla para los fundamentales (se usa la fecha más reciente del balance sheet)
create_fundamentals = """
CREATE TABLE IF NOT EXISTS Fundamentales (
    ticker VARCHAR PRIMARY KEY,
    assets NUMERIC,
    debt NUMERIC,
    invested_capital NUMERIC,
    share_issued NUMERIC
);
"""

# Tabla para los tenedores institucionales
create_holders = """
CREATE TABLE IF NOT EXISTS Tenedores (
    ticker VARCHAR,
    date_reported DATE,
    holder TEXT,
    shares NUMERIC,
    value NUMERIC,
    PRIMARY KEY (ticker, date_reported, holder)
);
"""

cursor.execute(create_basic_info)
cursor.execute(create_daily_prices)
cursor.execute(create_fundamentals)
cursor.execute(create_holders)
connection.commit()

# Lista de tickers de bancos
bancos_tickers = ['JPM', 'BAC', 'WFC', 'C', 'GS']

for ticker_symbol in bancos_tickers:
    banco = yf.Ticker(ticker_symbol)


    # 2. Extraer y cargar Informaciones Básicas
    info = banco.info
    insert_basic_info = """
    INSERT INTO Informaciones_Basicas (ticker, industry, sector, full_time_employees, city, phone, state, country, website, address)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker) DO NOTHING;
    """
    basic_info_data = (
        ticker_symbol, 
        info.get('industry'),
        info.get('sector'),
        info.get('fullTimeEmployees'),
        info.get('city'),
        info.get('phone'),
        info.get('state'),
        info.get('country'),
        info.get('website'),
        info.get('address1')
    )
    cursor.execute(insert_basic_info, basic_info_data)
    connection.commit()

    # 3. Extraer y cargar Precio Diario en Bolsa
    hist = banco.history(period="1d")
    insert_daily_prices = """
    INSERT INTO Precio_Diario_Bolsa (ticker, date, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    for row in hist.itertuples():
        date_value = row.Index.date()
        data_daily = (
            ticker_symbol,
            date_value,
            row.Open,
            row.High,
            row.Low,
            row.Close,
            row.Volume
        )
        cursor.execute(insert_daily_prices, data_daily)
    connection.commit()

    # 4. Extraer y cargar Fundamentales
    try:
        # Se obtiene la fecha más reciente del balance sheet
        fechas = pd.to_datetime(banco.balance_sheet.columns, errors='coerce')
        fecha_mayor = fechas[fechas.notna()].max().strftime('%Y-%m-%d')
        columna_mayor = banco.balance_sheet[fecha_mayor]
        transpuesto = columna_mayor.to_frame().T
        insert_fundamentals = """
        INSERT INTO Fundamentales (ticker, assets, debt, invested_capital, share_issued)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (ticker) DO NOTHING;
        """
        fundamentals_data = (
            ticker_symbol,
            transpuesto['Total Assets'].item() if 'Total Assets' in transpuesto.columns else None,
            transpuesto['Total Debt'].item() if 'Total Debt' in transpuesto.columns else None,
            transpuesto['Invested Capital'].item() if 'Invested Capital' in transpuesto.columns else None,
            transpuesto['Share Issued'].item() if 'Share Issued' in transpuesto.columns else None
        )
        cursor.execute(insert_fundamentals, fundamentals_data)
        connection.commit()
    except Exception as e:
        print(f"Error extrayendo fundamentales para {ticker_symbol}: {e}")

    # 5. Extraer y cargar Tenedores (Institutional Holders)
    holders = banco.institutional_holders
    if holders is not None:
        insert_holders = """
        INSERT INTO Tenedores (ticker, date_reported, holder, shares, value)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        for _, row in holders.iterrows():
            try:
                date_reported = pd.to_datetime(row['Date Reported']).date()
            except Exception:
                date_reported = None
            data_holder = (
                ticker_symbol,
                date_reported,
                row['Holder'],
                row['Shares'],
                row['Value']
            )
            cursor.execute(insert_holders, data_holder)
        connection.commit()

cursor.close()
connection.close()
print("Proceso de extracción e inserción completado.")


