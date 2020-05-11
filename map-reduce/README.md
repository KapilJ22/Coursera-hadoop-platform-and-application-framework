# Map/Reduce

The Map/Reduce framework requires mapper and reducer functions, where the map function reads in the data and outputs (key, value) pairs. Hadoop then shuffles and groups the (key, value) pairs so that all pairs with the same key are grouped together and passed to the same reducer. The reducer then reads in the intermediary data and processes the results. The key contribution of the map/reduce framework is its scalability and fault tolerance as it uses distributed servers and runs the various tasks in parallel.
