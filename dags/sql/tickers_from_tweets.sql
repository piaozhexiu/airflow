INSERT INTO fast_campus.default.tickers_from_tweets
SELECT
    date,
    hour,
    explode(regexp_extract_all(tweet_text, '\\$([A-Z]+)')) as ticker,
    count(1) as count
FROM
    fast_campus.default.tweet_text
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
