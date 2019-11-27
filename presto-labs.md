
# Presto Labs

## Data
- We have a table contains post informations of Stackoverflow's users: posts.csv:

| id |user_id  |title|created_at
|--|--|--|--
| 57743415 | 11435841 |How to play one mp4 video in MacOs|2019-09-01T05:23:51.327
|57743416|4687359|Copy Bower components' css, js and scss into another directory with Gulp|2019-09-01T05:23:41.240
|57743417|5935710|Excel VBA QueryTable for new connection to Access db|2019-09-02T07:12:40.345

- Table contains user regions of Stackoverflow's users: users.csv. We have 18 different regions
  
| user_id |region
|--|--
| 9003279 | KA
| 2777687 | NY

- And table comments contains comments of each post: comments.csv
  
| id | user_id | post_id |created_at
|--|--|--|--
| 100589675 | 330315 | 57029979 | 2019-07-14T19:12:17.113
| 100589676 | 913810 | 24027011 | 2019-07-14T19:12:18.937
| 100589677 | 10417280 | 56964710 | 2019-07-14T19:12:34.750

- Data input:
  - **labs/src/resources/posts.csv** (~800k rows)
  - **labs/src/resources/users.csv** (~18k rows)
  - **labs/src/resources/comments.csv** (~10k rows)
- Data input HDFS:
  - **/data/spark-labs/input/posts.csv** (~3M rows)
  - **/data/spark-labs/input/users.csv** (~18k rows)
  - **/data/spark-labs/input/comments.csv** (~1M rows)
  
## Exercise
- Exercise 1: Count number of users have any posts in each day.
  - Expected output: with column **count** is distinct users in day col **date**
  - Use both function distinct and approximate_distinct of presto to compare results.

| date |count
|--|--  
| 2019-09-02 | 3000
| 2019-09-01 | 2100

- Exercise 2: Count number of posts in each day of each user region.
    - Expected output: Values in col (AL, AM, CA, NY, ...) are count post of where owner is in.

| date | AL | AM | CA | NY | ...
|--|--|--|--|--|--
| 2019-09-02 | 300 | 200 | 100 | 300 | ...
| 2019-09-01 | 210 | 190 | 100 | 210 | ...

- Exercise 3: Count number of posts have at least one comment in month.
    - Expected output: **month**, and number of posts
    - Example: Have 3800 posts which have at least one comment in 2019-09

| month | posts
|--|--
|2019-09| 3800
|2019-08| 2623
|2019-07| 1727

- Exercise 4: Count number of comments all users have at least one post in month
    - Expected output: **month**, and number of comments
    - Use Subquery and Create View features of presto to compare results.
    - Example: Have 380 comments of all users which have at least one post in month 2019-09

| month | comments
|--|--
|2019-09| 380
|2019-08| 350