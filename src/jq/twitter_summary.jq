[
{tweetcount: length, runname: $runname} +
(group_by(.candidate) | .[] |
  ({ name: first.candidate, idcount: length}) +
  (unique_by(.uid) | { user_idcount: length }) +
  (unique_by(.rt_id) | { rt_idcount: length }) +
  ([.[].rt_rtc] | { rt_rtcount: max }) +
  (map({date, utime: .date | strptime("%a %b %d %H:%M:%S %z %Y") | mktime }) |
    { min_datetime: min_by(.utime) | .date,
      max_datetime: max_by(.utime) | .date
    }
  )
)
]
