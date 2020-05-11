# Map/Reduce - Simple Join
We have 2 files - fileA and fileB. FileA has total word count (word, total-count) and fileB has word count by date word combination (date word, day-count). We will conduct an inner join, matching words in fileA to words in fileB and merge those matches to produce an output of (date word, day-count total-count).

   - The mapper emits (word, total-count) as the (key, value) for fileA and emits (word, date day-count) as the (key, value) for fileB by moving the date to the value field
   - The reducer receives the shuffled (key, value) pairs, joins the data, and puts the date back into the key to produce an output of (date word, day-count total-count).
