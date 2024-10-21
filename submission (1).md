
# Applied Data Analytics

## Wedge Project

This project was tough! But I really enjoyed the work and I am looking forward to applying the skills to try out some ideas I have been meaning to work on.

<!-- Any general commentary you'd like to say about the project --> 

### Task 1

* Files for this task: 

    ***TaskOneNEW.ipynb***

    ***data_functions.py***




Loads all data into GBQ data set.

`TaskOneNEW.ipynb`: 
This file unzips the data, determines the header and delimeter of each file, then uses the data_functions.py file to clean and load the data into GBQ.

`data_functions.py`: 
This file contains two functions that clean the data, then take a pandas dataframe and loads it into a GBQ table.


### Task 2

* Files for this task: 
    
    ***tasktwo.ipynb***

    ***transactions.txt***

Queries data in GBQ data set and creates a sample file.

`tasktwo.ipynb`: 
This file connects to the GBQ data set, then runs a query to get a list of all owners within the data set (not inscluding owner 3). Once the list is pulled down, we sample it to get a list of around 400 ownwers. Then we run a query to get all the data for those owners and save it to a txt file called transactions.txt.  

`transactions.txt`:
This file contains all the transaction data for the 400 owners that were sampled in the tasktwo.ipynb file.
	

### Task 3

* Files for this task: 

    ***taskthree.ipynb*** 

    ***wedge.db***  

Queries data from GBQ and loads it into a SQLite database.

`taskthree.ipynb`: 
This file performs three seperate queries on the GBQ data set. After the queries are run, the data is loaded into pandas dataframes. The dataframes are then loaded into a SQLite database called wedge.db.

`wedge.db`:
This file contains three tables, one for each query that was run in the taskthree.ipynb file.


## Query Comparison Results

Fill in the following table with the results from the 
queries contained in `gbq_assessment_query.sql`. You only
need to fill in relative difference on the rows where it applies. 
When calculating relative difference, use the formula 
` (your_results - john_results)/john_results)`. 



|  Query  |  Your Results  |  John's Results | Difference | Rel. Diff | 
|---|---|---|---|---|
| Total Rows  | 85760139  | 85760139  | 0  | 0  |
| January 2012 Rows  | 1070907  |  1070907 | 0  | 0  |
| October 2012 Rows  |  1042287 | 1042287  | 0  | 0  |
| Month with Fewest  |  FEB |  FEB | No  | NA  |
| Num Rows in Month with Fewest  | 6556770  | 6556770 |  0 |  0 |
| Month with Most  | May  | May  | No  | NA  |
| Num Rows in Month with Most  | 7578372  | 7578372  | 0  | 0  |
| Null_TS  | 7207221  | 7123792  |  83429  | 0.01171131891  |
| Null_DT  |  0 | 0  |  0 | 0  |
| Null_Local  | 234839  | 234843  | -4  |  -.00001703 |
| Null_CN  | 0  | 0  |   |   |
| Num 5 on High Volume Cards  |  14987.0 | 14987.0  | No  | NA  |
|  Num Rows for Number 5 |  460625 | 460630 | -5  | -0.000011  |
| Num Rows for 18736  | 12153  | 12153  |   |   |
| Product with Most Rows  |  banana organic |  banana organic | No  | NA  |
| Num Rows for that Product  | 908639  | 908639  | 0  |  0 |
| Product with Fourth-Most Rows  | avocado hass organic  |  avocado hass organic | No  | NA  |
| Num Rows for that Product  | 456771  |  456771 | 0  | 0  |
| Num Single Record Products  | 2741  | 2769  | -28  | -0.010  |
| Year with Highest Portion of Owner Rows  | 2017  |  2017 | No  | NA |
| Fraction of Rows from Owners in that Year  | 0.7513  | 0.7513  | 0  |  0 |
| Year with Lowest Portion of Owner Rows  | 2010  | 2010  | Yes/No  | NA |
| Fraction of Rows from Owners in that Year  | 0.7422  | 0.7422  |  0 |  0 |

## Reflections

The Wedge project was a great opportunity to work with a large data set with a variety of data types. It was a really great way to apply the skills and knowledge that I have been working on throughout the semester. There was a lot of trying and failing, but I learned a lot from the process. I think the most challenging part of the project was working with the large data set and trying to figure out the best way to clean and load the data. I think the most rewarding part of the project was seeing my numbers slowly start to match up with the 'example' dataset numbers.

Going forward into the capstone project, this project will be a great reference point for me. I intend to clean up my notebooks as well to make them more readable and easier to follow. I want to work on including better logging and error handling in my code as well. I think that will be a big help in the capstone project.
<!-- I'd love to get 100-200 words on your experience doing the Wedge Project --> 
