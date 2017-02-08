map({ candidate, hashtags, rtc, rt_rtc, fav, rt_fav, ufol, rt_ufol }) |
map(select(.hashtags | length > 0))
