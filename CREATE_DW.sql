-- Create Data Warehouse Database
IF NOT EXISTS (
    SELECT name FROM sys.databases WHERE name = 'flight_dataWarehouse'
)
BEGIN
    CREATE DATABASE flight_dataWarehouse;
END
GO

USE flight_dataWarehouse;
GO

-- Dimension Table: DimDate
CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY, -- e.g. YYYYMMDD
    Year INT,
    Month INT,
    Day INT
);
GO

-- Dimension Table: DimAirline
CREATE TABLE DimAirline (
    AirlineKey INT IDENTITY(1,1) PRIMARY KEY,
    IATA_CODE VARCHAR(10),
    Airline NVARCHAR(100)
);
GO

-- Dimension Table: DimAirport
CREATE TABLE DimAirport (
    AirportKey INT IDENTITY(1,1) PRIMARY KEY,
    IATA_CODE VARCHAR(10),
    Airport NVARCHAR(100),
    City NVARCHAR(50),
    State NVARCHAR(50)
);
GO

-- Fact Table: FactFlight
CREATE TABLE FactFlight (
    FlightID INT IDENTITY(1,1) PRIMARY KEY,
    DateKey INT,
    AirlineKey INT,
    OriginAirportKey INT,
    DestAirportKey INT,
    Distance INT,
    ArrivalDelay INT,
    DepartureDelay INT,
    CancelledFlag BIT,
    DepartureTime TIME,
    CancelReason NVARCHAR(50),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (AirlineKey) REFERENCES DimAirline(AirlineKey),
    FOREIGN KEY (OriginAirportKey) REFERENCES DimAirport(AirportKey),
    FOREIGN KEY (DestAirportKey) REFERENCES DimAirport(AirportKey)
);
GO

-- Indexes for faster OLAP queries
CREATE INDEX idx_factflight_datekey ON FactFlight(DateKey);
CREATE INDEX idx_factflight_airlinekey ON FactFlight(AirlineKey);
CREATE INDEX idx_factflight_originairportkey ON FactFlight(OriginAirportKey);
CREATE INDEX idx_factflight_destairportkey ON FactFlight(DestAirportKey);
GO