SELECT
  INSTNM
  , ADM_RATE_ALL
  , FIRST_GEN
  , MD_FAMINC
  , MD_EARN_WNE_P10 , SAT_AVG
FROM
  ch04.college_scorecard
WHERE
  SAT_AVG > 1300
  AND ADM_RATE_ALL < 0.2
  AND FIRST_GEN > 0.1
ORDER BY
  MD_FAMINC ASC