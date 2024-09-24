#----- QUERIES -----#

all_quote_dates_SPX_EOD = """
SELECT distinct(quote_date) FROM landing.EOD
where underlying_symbol = '^SPX'
order by quote_date asc;
"""

all_quote_dates_VIX_EOD = """
SELECT distinct(quote_date) FROM landing.EOD
where underlying_symbol = '^VIX'
order by quote_date asc;
"""

all_quote_dates_OC = """
SELECT distinct(quote_date) FROM landing.OC
order by quote_date asc;
"""

latest_quote_date = """
SELECT max(quote_date) FROM landing.OC
"""

# Query permettant de trouves les file_dates non simulés
non_simulated_dates = """
WITH
  cte_eod AS (SELECT DISTINCT(quote_date) FROM landing.EOD),
  cte_sim AS (SELECT DISTINCT(book_date) FROM results.hlm)
SELECT quote_date FROM cte_eod 
LEFT JOIN cte_sim
ON cte_eod.quote_date = cte_sim.book_date
WHERE cte_sim.book_date IS NULL
"""

tables_sync_check = """
WITH  
	cte_quote_date_eod as (select max(quote_date) as quote_date from landing.EOD),
	cte_quote_date_oc as (select max(quote_date)as quote_date from landing.OC)
SELECT  *
FROM    cte_quote_date_eod
UNION ALL
SELECT  *
FROM    cte_quote_date_oc
"""


eod_price_for_book = """
SELECT distinct(active_underlying_price_1545) FROM landing.EOD
where quote_date =%(book_date)s;
"""

log_IV = """
SELECT * from logs.log_IV
"""

m1_OHLC = """
SELECT * FROM defaultdb.OHLCV_1m 
where symbol = %(ticker)s 
and DATE(datetime) = %(effective_date)s;
"""

all_mm_books_dates = """
SELECT distinct(effective_date) from results.mm_books
"""

get_book = """
SELECT * from optionsdepth_stage.charts_mmbook
where effective_date = %(effective_date)s
and
ticker = %(ticker)s
"""

get_book_as_of= """
SELECT * from optionsdepth_stage.charts_mmbook
where as_of_date = %(as_of_date)s
and
ticker = %(ticker)s
"""


specific_book ="""
-- Common Table Expression 2: Calculate various metrics for options trading data
WITH cte_mm_book AS (
    SELECT
        %(book_date)s AS "book_date",
        "landing"."OC"."underlying_symbol" AS "underlying_symbol",
        "landing"."OC"."option_symbol" AS "option_symbol",
        "landing"."OC"."call_put_flag" AS "call_put_flag",
        "landing"."OC"."expiration_date" AS "expiration_date",
        "landing"."OC"."strike_price" AS "strike_price",
        SUM("landing"."OC"."mm_buy_vol") AS "bought",
        SUM("landing"."OC"."mm_sell_vol") AS "sold",
        (SUM("landing"."OC"."mm_buy_vol") - SUM("landing"."OC"."mm_sell_vol")) AS "net_contract"
    FROM "landing"."OC"
    WHERE "landing"."OC"."quote_date" <= %(book_date)s             -- Filter on the quote date prior to the wanted book_date
    AND "landing"."OC"."expiration_date" > %(book_date)s         -- Filter by the provided date
    AND "landing"."OC"."underlying_symbol" = %(ticker)s             -- Filter by the provided ticker
    GROUP BY
        "landing"."OC"."underlying_symbol",
        "landing"."OC"."option_symbol",
        "landing"."OC"."call_put_flag",
        "landing"."OC"."expiration_date",
        "landing"."OC"."strike_price"
),


-- Main Query: Join the calculated data with "SPX_EOD" table and filter by net_contract != 0
cte_join_eod AS(
    SELECT
    "cte_mm_book"."book_date" AS "book_date",
    "cte_mm_book"."underlying_symbol" AS "underlying_symbol",
    "cte_mm_book"."option_symbol" AS "option_symbol",
    "cte_mm_book"."call_put_flag" AS "call_put_flag",
    "cte_mm_book"."expiration_date" AS "expiration_date",
    "cte_mm_book"."strike_price" AS "strike_price",
    "cte_mm_book"."net_contract" AS "net_contract",
    "landing"."EOD"."open_interest" AS "open_interest",
    "landing"."EOD"."implied_volatility_1545" AS "implied_volatility_1545",
    "landing"."EOD"."implied_underlying_price_1545" AS "implied_underlying_price_1545",
    "landing"."EOD"."active_underlying_price_1545" AS "active_underlying_price_1545",
    "landing"."EOD"."delta_1545" AS "delta_1545",
    "landing"."EOD"."gamma_1545" AS "gamma_1545"
FROM
    "cte_mm_book"
JOIN
    "landing"."EOD" ON (
        "cte_mm_book"."book_date" = "landing"."EOD"."quote_date" AND
        "cte_mm_book"."underlying_symbol" = "landing"."EOD"."underlying_symbol" AND
        "cte_mm_book"."option_symbol" = "landing"."EOD"."root" AND
        "cte_mm_book"."call_put_flag" = "landing"."EOD"."option_type" AND
        "cte_mm_book"."expiration_date" = "landing"."EOD"."expiration" AND
        "cte_mm_book"."strike_price" = "landing"."EOD"."strike"
    )
WHERE "cte_mm_book"."expiration_date" > "cte_mm_book"."book_date"
AND "cte_mm_book"."net_contract" <> 0             -- Filter by net_contract not equal to 0
ORDER BY
    "cte_mm_book"."expiration_date" ASC,         -- Ascending expiration_date
    "cte_mm_book"."net_contract" DESC)            -- Descending net_contract


-- Select data with the desired columns from the "cte_book" CTE and include the CASE expression
SELECT
    "cte_join_eod"."book_date",
    "cte_join_eod"."underlying_symbol",
    "cte_join_eod"."option_symbol",
    "cte_join_eod"."call_put_flag",
    "cte_join_eod"."expiration_date" as expiration_date_original,
        CASE
        WHEN "cte_join_eod"."option_symbol" IN ('SPX', 'VIX') THEN STR_TO_DATE(CONCAT("cte_join_eod"."expiration_date", ' 09:15:00'), '%Y-%m-%d %H:%i:%s')
        WHEN "cte_join_eod"."option_symbol" IN ('SPXW', 'VIXW') THEN STR_TO_DATE(CONCAT("cte_join_eod"."expiration_date", ' 16:00:00'), '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS "expiration_date",
    "cte_join_eod"."strike_price",
    "cte_join_eod"."net_contract",
    "cte_join_eod"."open_interest",
    "cte_join_eod"."implied_volatility_1545",
    "cte_join_eod"."implied_underlying_price_1545",
    "cte_join_eod"."active_underlying_price_1545",
    "cte_join_eod"."delta_1545",
    "cte_join_eod"."gamma_1545"
FROM cte_join_eod;
"""

all_dates_SPX_exposure_moneyness = """
SELECT distinct(as_of_date) FROM results.exposure_moneyness
where ticker = 'SPX'
order by as_of_date asc;
"""

is_date_in_books = """
SELECT COUNT(*) FROM intraday.new_daily_book_format
-- SELECT COUNT(*) FROM optionsdepth_stage.charts_mmbook
WHERE as_of_date = %(quote_date)s
and
ticker = %(ticker)s
"""

is_date_in_pg_books = """
SELECT COUNT(*) FROM public.charts_dailybook
WHERE as_of_date = %(quote_date)s
and
ticker = %(ticker)s
"""

not_in_mm_books = """
WITH
  cte_mm_book AS (SELECT DISTINCT(as_of_date) FROM optionsdepth_stage.charts_mmbook),
  cte_OC AS (SELECT DISTINCT(quote_date) FROM landing.OC)
SELECT as_of_date FROM cte_mm_book 
LEFT JOIN cte_OC
ON cte_mm_book.as_of_date = cte_OC.quote_date
WHERE cte_OC.quote_date IS NULL
"""

delete_mm_book_entry = """
DELETE FROM optionsdepth_stage.charts_mmbook 
where as_of_date = %(as_of_date)s 
and 
ticker = %(ticker)s
"""

get_candlesticks = """
SELECT t1.*
FROM defaultdb.OHLCV_1m t1
INNER JOIN (
    SELECT datetime, MAX(update_time) as max_update_time
    FROM defaultdb.OHLCV_1m
    WHERE symbol = %(symbol)s AND DATE(datetime) = %(date)s
    GROUP BY datetime
) t2 ON t1.datetime = t2.datetime AND t1.update_time = t2.max_update_time
WHERE t1.symbol = %(symbol)s
ORDER BY t1.datetime;
"""

new_specific_book = """
WITH participant_net_positions AS (
    SELECT
        %(book_date)s AS "book_date",
        "landing"."OC"."underlying_symbol" AS "underlying_symbol",
        "landing"."OC"."option_symbol" AS "option_symbol",
        "landing"."OC"."call_put_flag" AS "call_put_flag",
        "landing"."OC"."expiration_date" AS "expiration_date",
        "landing"."OC"."strike_price" AS "strike_price",
        -- Market Maker Net Position
        (SUM("landing"."OC"."mm_buy_vol") - SUM("landing"."OC"."mm_sell_vol")) AS "mm_posn",
        -- Firm Net Position
        (SUM("landing"."OC"."firm_open_buy_vol") + SUM("landing"."OC"."firm_close_buy_vol") - 
         SUM("landing"."OC"."firm_open_sell_vol") - SUM("landing"."OC"."firm_close_sell_vol")) AS "firm_posn",
        -- Broker/Dealer Net Position
        (SUM("landing"."OC"."bd_open_buy_vol") + SUM("landing"."OC"."bd_close_buy_vol") - 
         SUM("landing"."OC"."bd_open_sell_vol") - SUM("landing"."OC"."bd_close_sell_vol")) AS "broker_posn",
        -- Customer Net Position
        (SUM("landing"."OC"."cust_lt_100_open_buy_vol") + SUM("landing"."OC"."cust_lt_100_close_buy_vol") + 
         SUM("landing"."OC"."cust_100_199_open_buy_vol") + SUM("landing"."OC"."cust_100_199_close_buy_vol") + 
         SUM("landing"."OC"."cust_gt_199_open_buy_vol") + SUM("landing"."OC"."cust_gt_199_close_buy_vol") - 
         SUM("landing"."OC"."cust_lt_100_open_sell_vol") - SUM("landing"."OC"."cust_lt_100_close_sell_vol") - 
         SUM("landing"."OC"."cust_100_199_open_sell_vol") - SUM("landing"."OC"."cust_100_199_close_sell_vol") - 
         SUM("landing"."OC"."cust_gt_199_open_sell_vol") - SUM("landing"."OC"."cust_gt_199_close_sell_vol")) AS "nonprocust_posn",
        -- Professional Customer Net Position
        (SUM("landing"."OC"."procust_lt_100_open_buy_vol") + SUM("landing"."OC"."procust_lt_100_close_buy_vol") + 
         SUM("landing"."OC"."procust_100_199_open_buy_vol") + SUM("landing"."OC"."procust_100_199_close_buy_vol") + 
         SUM("landing"."OC"."procust_gt_199_open_buy_vol") + SUM("landing"."OC"."procust_gt_199_close_buy_vol") - 
         SUM("landing"."OC"."procust_lt_100_open_sell_vol") - SUM("landing"."OC"."procust_lt_100_close_sell_vol") - 
         SUM("landing"."OC"."procust_100_199_open_sell_vol") - SUM("landing"."OC"."procust_100_199_close_sell_vol") - 
         SUM("landing"."OC"."procust_gt_199_open_sell_vol") - SUM("landing"."OC"."procust_gt_199_close_sell_vol")) AS "procust_posn"
    FROM "landing"."OC"
    WHERE "landing"."OC"."quote_date" <= %(book_date)s
    AND "landing"."OC"."expiration_date" > %(book_date)s
    AND "landing"."OC"."underlying_symbol" = %(ticker)s 
    GROUP BY
        "landing"."OC"."underlying_symbol",
        "landing"."OC"."option_symbol",
        "landing"."OC"."call_put_flag",
        "landing"."OC"."expiration_date",
        "landing"."OC"."strike_price"
),

total_net_positions AS (
    SELECT
        *,
        ("firm_posn" + "broker_posn" + "nonprocust_posn" + "procust_posn") AS "total_customers_posn"
    FROM participant_net_positions
    HAVING "mm_posn" <> 0 OR "firm_posn" <> 0 OR "broker_posn" <> 0 OR "nonprocust_posn" <> 0 OR "procust_posn" <> 0
),

cte_join_eod AS (
    SELECT
        total_net_positions.*,
        "landing"."EOD"."implied_volatility_1545" AS "iv",
        "landing"."EOD"."delta_1545" AS "delta",
        "landing"."EOD"."gamma_1545" AS "gamma",
        "landing"."EOD"."vega_1545" AS "vega"
    FROM
        total_net_positions
    JOIN
        "landing"."EOD" ON (
            "total_net_positions"."book_date" = "landing"."EOD"."quote_date" AND
            "total_net_positions"."underlying_symbol" = "landing"."EOD"."underlying_symbol" AND
            "total_net_positions"."option_symbol" = "landing"."EOD"."root" AND
            "total_net_positions"."call_put_flag" = "landing"."EOD"."option_type" AND
            "total_net_positions"."expiration_date" = "landing"."EOD"."expiration" AND
            "total_net_positions"."strike_price" = "landing"."EOD"."strike"
        )
    ORDER BY
        "total_net_positions"."expiration_date" ASC,         -- Ascending expiration_date
        "total_net_positions"."mm_posn" DESC                 -- Descending net_contract
)

-- Select data with the desired columns from the "cte_join_eod" CTE and include the CASE expression
SELECT
    "cte_join_eod"."book_date" AS "as_of_date",
    "cte_join_eod"."underlying_symbol",
    "cte_join_eod"."option_symbol",
    "cte_join_eod"."call_put_flag",
    "cte_join_eod"."expiration_date" as "expiration_date_original",
    CASE
        WHEN "cte_join_eod"."option_symbol" IN ('SPX', 'VIX') THEN STR_TO_DATE(CONCAT("cte_join_eod"."expiration_date", ' 09:15:00'), '%Y-%m-%d %H:%i:%s')
        WHEN "cte_join_eod"."option_symbol" IN ('SPXW', 'VIXW') THEN STR_TO_DATE(CONCAT("cte_join_eod"."expiration_date", ' 16:00:00'), '%Y-%m-%d %H:%i:%s')
        ELSE NULL
    END AS "expiration_date",
    "cte_join_eod"."strike_price",
    "cte_join_eod"."mm_posn",
    "cte_join_eod"."firm_posn",
    "cte_join_eod"."broker_posn",
    "cte_join_eod"."nonprocust_posn",
    "cte_join_eod"."procust_posn",
    "cte_join_eod"."total_customers_posn",
    "cte_join_eod"."iv",
    "cte_join_eod"."delta",
    "cte_join_eod"."gamma",
    "cte_join_eod"."vega"
FROM cte_join_eod;
"""