[.search_metadata.query as $q | 
  .statuses[] | 
  { q: $q, id, text, date: .created_at, fav: .favorite_count, rtc: .retweet_count,
    u: .user.screen_name, uid: .user.id, ufol: .user.followers_count,
    hashtags: [.entities.hashtags[].text],
    rt_id: .retweeted_status.id, rt_text: .retweeted_status.text, rt_date: .retweeted_status.created_at, rt_fav: .retweeted_status.favorite_count, rt_rtc: .retweeted_status.retweet_count,
      rt_u: .retweeted_status.user.screen_name, rt_uid: .retweeted_status.user.id, rt_ufol: .retweeted_status.user.followers_count
  }  
]