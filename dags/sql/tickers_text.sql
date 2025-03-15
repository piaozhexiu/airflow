INSERT INTO fast_campus.default.tweet_text
SELECT
  date,
  hour,
  tweet.text as tweet_text
FROM
  (
    SELECT
      DATE(_airbyte_extracted_at) as date,
      HOUR(_airbyte_extracted_at) as hour,
      explode(
        from_json(data, 'array<struct<id:string, text:string, edit_history_tweet_ids:array<string>>>')
      ) AS tweet
    FROM
      fast_campus.default.tweet_search
  )
