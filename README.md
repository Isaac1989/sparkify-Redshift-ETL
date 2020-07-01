## PROBLEM STATEMENT
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


## APPROACH TO SOLUTION
I solved the problem by doing the following:
1. Designing two tables `stage_events` and `stage_songs`on Amazon Redshift  meant for loading data from AWS S3 bucket that houses the raw json files
2. Next, dimension tables `users`, `song`, `artist`, and `times` were designed and created together with the fact table `songplay`. These five tables were used to define a star Schema. The choice of star schema is predominantly selected because it involves fewer joins which is great for data analytics. Because it involves few join, it is also fast to query.
3. Finally, I designed and implemented an ETL process to extract the data from the AWS S3 bucket. The data was subsequently transformed before being loaded into the AWS redshift.

## HOW TO RUN THE SCRIPTS
1. Launch a redshift cluster on AWS
2. fill out `dwh.cfg` your datbase credentials
3. Run create_table in the python shell e.g ```python python create_tables.py```
4. Run `etl.py` like so `python etl.py`. 
5. Check your AWS Redshift for loaded data. You should be able to query it in the editor on AWS redshift cluster.

##  SAMPLE QUERY
The star schema makes it easy to find insight in the data. As an example, the top 20 most active free users can be queried as:

```sql 
    with active_free_users as
    (select
    user_id,
    count(*) "freq"
    from songplay 
    where level = 'free'
    group by  user_id
     )
    select
    distinct u.user_id,
    first_name || ' ' || last_name "full_name",
    freq
    from users u
    join active_free_users a
    on u.user_id = a.user_id
    order by freq desc
    limit 20;
```

The query shows that **Ava Robinson** is the most frequent free users.
