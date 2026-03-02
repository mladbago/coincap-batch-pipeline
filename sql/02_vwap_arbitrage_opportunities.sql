-- Identify assets with the highest divergence from their 24h VWAP
SELECT
    name,
    symbol,
    ROUND(priceUsd, 4) AS current_price,
    ROUND(vwap24Hr, 4) AS vwap_24h,
    ROUND(changePercent24Hr, 2) AS standard_daily_change_pct,
    -- Calculate the spread between current spot price and volume-weighted average
    ROUND(((priceUsd - vwap24Hr) / vwap24Hr) * 100, 2) AS vwap_spread_pct,
    CASE
        WHEN priceUsd > vwap24Hr THEN 'Trading at Premium'
        ELSE 'Trading at Discount'
    END AS market_status
FROM coincap_raw
WHERE year = '2026' AND month = '03' AND day = '02'
    AND vwap24Hr IS NOT NULL
    AND volumeUsd24Hr > 10000000 -- Filter out low-volume noise
ORDER BY ABS(((priceUsd - vwap24Hr) / vwap24Hr)) DESC
LIMIT 15;