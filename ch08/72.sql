SET @@time_zone = "America/New_York";
SELECT FORMAT_TIMESTAMP("%c", TIMESTAMP "2008-12-25 15:00:00 UTC");