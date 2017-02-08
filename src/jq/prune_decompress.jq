# Decompress tweets compressed with prune_compress
map(
  . as $tweet
    | if has("qs") then
      .qs | map($tweet + {q: .} | del(.qs))
    else
      $tweet
    end
)
  | flatten
