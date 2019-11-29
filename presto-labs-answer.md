# Presto Labs

## Exercise 1

- Use Count Distinct
  
``` sql
SELECT substr(created_at, 1, 10) as date, count(distinct user_id) as users
FROM posts
GROUP BY 1
ORDER BY 1 DESC
LIMIT 20
```

- Use Approx Distinct

``` sql
SELECT substr(created_at, 1, 10) as date, approx_distinct(user_id)
FROM posts
GROUP BY 1
ORDER BY 1 DESC
LIMIT 20
```

## Exercise 2

```sql
SELECT substr(created_at, 1, 10) as date,
count(case when region = 'AL' then 1 end) as AL,
count(case when region = 'AK' then 1 end) as AM,
count(case when region = 'CA' then 1 end) as CA,
count(case when region = 'NY' then 1 end) as NY
FROM posts JOIN users ON users.user_id = posts.user_id
GROUP BY 1
ORDER BY 1 DESC
LIMIT 20
```

## Exercise 3

```sql
SELECT substr(posts.created_at, 1, 7) as month,
count(distinct post_id) as number_of_posts
FROM comments JOIN posts ON posts.id = comments.post_id
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;
```

## Exercise 4

- Using sub query

```sql
SELECT substr(comments.created_at, 1, 7) AS month, count(comments.user_id) AS number_of_comments
FROM comments JOIN
(SELECT substr(created_at, 1, 7) AS month, user_id FROM posts GROUP BY 1,2)
AS sub ON comments.user_id = sub.user_id and substr(comments.created_at, 1, 7) = sub.month
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;
```

- Using Create View

```sql
CREATE VIEW sub AS (SELECT substr(created_at, 1, 7) AS month, user_id FROM posts GROUP BY 1,2);

SELECT substr(created_at, 1, 7) AS month,
count(comments.user_id) AS number_of_comments
FROM comments JOIN sub on comments.user_id = sub.user_id and substr(comments.created_at, 1, 7) = sub.month
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;
```