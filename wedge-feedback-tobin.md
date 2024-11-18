## Feedback 

Nice work on this project, you can consider it complete. I'm going to read your files in order and give you feedback as I move through them, from Task 1 to Task 3. 

Let me start by saying, I appreciate the various notes you dropped throughout the docs. To the cloud! Having said that, your file names are probably a smidge too informal. If your submission file is called `submission (1).md` then it gives the impression you're not paying close attention to your files, their names, and how often you download them. (It's fine to do the multiple downloads, just adjust the names.) 

I notice you call out logging at the end of the doc. This is a great idea. Check out the package `logger`. I've played around with it and used it some. If we had more time I'd cover it explicitly, but I'm already nervous I'm cramming in too much. 


### Task 1

* Cool use of the sniffer `has_header` function. I'm not sure I knew that existed. 
* Impressive use of the data functions. That'd make your code much easier to maintain. 
* Overall, very nice work on this task.

### Task 2

* This isn't a great way to sample: `owner_sample = owners[:400]`. I suspect you're getting the smallest numbered owners. If that's the case, you might be biasing your sample toward owners who were created earlier. Much better to use `random.sample` after first calling `random.seed`. 
* Everything else looks great. 

### Task 3

* Nice job on this, very efficient. 
* Kudos for remembering or recreating the queries from Intro to SQL. *Many* of your classmates did not do that. 
