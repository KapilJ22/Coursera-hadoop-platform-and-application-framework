# Spark - Advanced Join
Using the data from the Join 2 exercise, the objective is to find out the total number of viewers across all shows for the channel BAT.

Parse show files:  
show_views_file = sc.textFile("input3/join2_gennum?.txt")  
show_views = show_views_file.map(split_show_views)

Parse channel files:  
show_channel_file = sc.textFile("input3/join2_genchan?.txt")  
show_channel = show_channel_file.map(split_show_channel)

Join the 2 datasets:  
joined_dataset = show_channel.join(show_views)

Extract channel as key:  
channel_views = joined_dataset.map(extract_channel_views)

Sum viewers across channels:  
channel_views.reduceByKey(sum_channel_views).collect()

Output: [channel_views_output.txt]
