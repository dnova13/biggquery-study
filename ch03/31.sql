SELECT
  UNIX_MILLIS(TIMESTAMP "2018-11-25 22:30:00 UTC")
  , UNIX_MILLIS(TIMESTAMP "1918-11-11 22:30:00 UTC") --유효하지 않다
  , TIMESTAMP_MILLIS(1543185000000)