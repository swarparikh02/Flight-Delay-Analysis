USE flight_dataWarehouse;


-- Average Delay per Route (Origin to Destination) - Y
SELECT o.Airport AS Origin, 
       o.State AS OriginState, 
       d.Airport AS Destination, 
       d.State AS DestinationState, 
       AVG(f.ArrivalDelay) AS AvgArrivalDelay
FROM FactFlight f
JOIN DimAirport o ON f.OriginAirportKey = o.AirportKey
JOIN DimAirport d ON f.DestAirportKey = d.AirportKey
GROUP BY o.Airport, o.State, d.Airport, d.State
HAVING COUNT(*) > 50
ORDER BY AvgArrivalDelay DESC;


SELECT 
    AirportName,
    State,
    AirportRole,
    AVG(Delay) AS AvgDelay,
    COUNT(*) AS NumFlights
FROM (
    -- Subquery to unify Origin and Destination delays into one set
    SELECT 
        o.Airport AS AirportName,
        o.State AS State,
        'Origin' AS AirportRole,
        f.DepartureDelay AS Delay
    FROM FactFlight f
    JOIN DimAirport o ON f.OriginAirportKey = o.AirportKey

    UNION ALL

    SELECT 
        d.Airport AS AirportName,
        d.State AS State,
        'Destination' AS AirportRole,
        f.ArrivalDelay AS Delay
    FROM FactFlight f
    JOIN DimAirport d ON f.DestAirportKey = d.AirportKey
) AS AirportDelays
GROUP BY AirportName, State, AirportRole
HAVING COUNT(*) > 50
ORDER BY AvgDelay DESC;


-- Total Distance by Route // To work
SELECT o.Airport AS Origin, d.Airport AS Destination, SUM(f.Distance) AS TotalDistance
FROM FactFlight f
JOIN DimAirport o ON f.OriginAirportKey = o.AirportKey
JOIN DimAirport d ON f.DestAirportKey = d.AirportKey
GROUP BY o.Airport, d.Airport
ORDER BY TotalDistance DESC;

-- Cancellation Rate per Route - Y
SELECT o.Airport AS Origin, d.Airport AS Destination, 
       SUM(CAST(f.CancelledFlag AS INT)) * 100.0 / COUNT(*) AS CancellationRate
FROM FactFlight f
JOIN DimAirport o ON f.OriginAirportKey = o.AirportKey
JOIN DimAirport d ON f.DestAirportKey = d.AirportKey
GROUP BY o.Airport, d.Airport
HAVING COUNT(*) > 50
ORDER BY CancellationRate DESC;

-- Average Delay by Airline by year and Month - Y
SELECT a.Airline, d.Year, d.Month, 
       AVG(f.ArrivalDelay) AS AvgArrivalDelay, 
       AVG(f.DepartureDelay) AS AvgDepartureDelay
FROM FactFlight f
JOIN DimAirline a ON f.AirlineKey = a.AirlineKey
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY a.Airline, d.Year, d.Month
ORDER BY d.Year, d.Month, AvgArrivalDelay DESC;

-- Total Flights per Airline by Year and Month - Y
SELECT a.Airline, d.Year, d.Month, COUNT(*) AS TotalFlights
FROM FactFlight f
JOIN DimAirline a ON f.AirlineKey = a.AirlineKey
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY a.Airline, d.Year, d.Month
ORDER BY d.Year, d.Month, TotalFlights DESC;

-- Cancellation Rate by Airline by year and Month - Y
SELECT a.Airline, d.Year, d.Month, 
       SUM(CAST(f.CancelledFlag AS INT)) * 100.0 / COUNT(*) AS CancellationRate
FROM FactFlight f
JOIN DimAirline a ON f.AirlineKey = a.AirlineKey
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY a.Airline, d.Year, d.Month
HAVING COUNT(*) > 100
ORDER BY d.Year, d.Month, CancellationRate DESC;

-- Cancellation Rate by Destination State - Y
SELECT d.State AS DestState, 
       SUM(CAST(f.CancelledFlag AS INT)) * 100.0 / COUNT(*) AS CancellationRate
FROM FactFlight f
JOIN DimAirport d ON f.DestAirportKey = d.AirportKey
GROUP BY d.State
HAVING COUNT(*) > 50
ORDER BY CancellationRate DESC;

-- Daily Cancellation Patterns - Y
SELECT d.Day, SUM(CAST(f.CancelledFlag AS INT)) AS DailyCancellations
FROM FactFlight f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Day
ORDER BY DailyCancellations DESC;



USE Flight_ś;