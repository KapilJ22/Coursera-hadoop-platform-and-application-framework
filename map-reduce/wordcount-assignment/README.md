# Map/Reduce - Wordcount
Wordcount is the paradigmatic example of Map/Reduce, which counts the number of occurrences of each word in a document.
  - The mapper emits (word, 1) as the  (key, value)
     * Get word
     * Emit (word, 1)
  - The reducer receives the shuffled (key, value) pairs and aggregates the values for all the same keys
     * Get next (word) (value)
     * If (word) is same as previous word
        * add (value) to count
     * else
        * emit (word, count)
        * set count to 0
