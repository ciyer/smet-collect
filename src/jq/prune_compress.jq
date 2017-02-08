# Compress tweets that appear in multiple searches for the same candidate
flatten
  | map(. + .q as $q | {candidate: $namemap[$q]})
  | group_by(.candidate)
  | map(group_by(.id))
  | map(map(first + {qs: map(.q)} | del(.q)))
  | flatten
