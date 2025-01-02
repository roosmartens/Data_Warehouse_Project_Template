# Introduction

This project is a data warehouse solution that leverages AWS Redshift for data storage and processing. The project includes several Python scripts to manage the infrastructure, perform ETL processes, and handle configuration settings.

## Project background

A music streaming startup, Sparkify, has grown their user base and song database and wants to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

# Files

## dwh.cfg

This file contains the configuration settings for the data warehouse project. It includes AWS credentials, Redshift cluster configuration, IAM role details, and S3 bucket paths for data.

## create_infra.py

This script sets up the infrastructure for the data warehouse project, including creating an IAM role and a Redshift cluster. This follows the IaC (Infrastructure As Code) principle, making it easy to delete / set up resources or making changes to them.

### Functions:
- `delete_infra()`: Starts the delete_infra.py script to clean up the infrastructure.
- `create_IAM_role()`: Creates an IAM role.
- `attach_policy()`: Attaches the necessary policy to the IAM role.
- `get_IAM_role_arn()`: Retrieves the IAM role ARN.
- `create_redshift_cluster(roleArn)`: Creates a Redshift cluster using the IAM role ARN.
- `main()`: Main function to set up the infrastructure for the data warehouse project.

## delete_infra.py

This script deletes the Redshift cluster and IAM role created for the data warehouse project.

### Functions:
- `delete_infra()`: Deletes the Redshift cluster and IAM role, and updates the configuration file.

## etl.py

This script performs the ETL (Extract, Transform, Load) process. It reads data from S3, loads it into staging tables in Redshift, and then inserts the data into the analytics tables.

### Functions:
- `load_staging_tables(cur, conn)`: Loads data from S3 to staging tables.
- `insert_tables(cur, conn)`: Inserts data from staging tables to analytics tables.
- `main()`: Main function to load data from S3 to staging tables and insert data from staging tables to analytics tables.

## sql_queries.py

This file contains SQL queries for creating, dropping, and inserting data into tables in a Redshift data warehouse. The tables are designed to support a star schema for a music streaming data pipeline.

### Variables:
- `create_table_queries`: List of SQL queries to create tables.
- `drop_table_queries`: List of SQL queries to drop tables.
- `copy_table_queries`: List of SQL queries to copy data from S3 to staging tables.
- `insert_table_queries`: List of SQL queries to insert data from staging tables to analytics tables.

Below is an explanation of the distribution strategies and key choices for the star schema tables:

### Staging tables
- **staging_events_table**: 
    - **Distribution style**: EVEN
    - **Reason**: The data is not skewed, so an even distribution ensures balanced data distribution across nodes.
    
- **staging_songs_table**: 
    - **Distribution style**: EVEN
    - **Reason**: Similar to the events table, an even distribution is chosen to ensure balanced data distribution.

### Fact table
- **songplays_table**:
    - **Distribution style**: KEY 
    - **Distribution key**: `song_id`
    - **Reason**: `song_id` is frequently used in join queries, making it an optimal choice for the distribution key.
    - **Sort key**: `start_time`
    - **Reason**: `start_time` is often used in the WHERE clause, making it an efficient choice for the sort key.

### Dimension tables
- **users_table**: 
    - **Distribution style**: ALL
    - **Reason**: The user table is relatively small, so distributing it to all nodes ensures fast access.
    
- **songs_table**: 
    - **Distribution style**: KEY 
    - **Distribution key**: `song_id`
    - **Reason**: `song_id` is frequently used in join queries, making it an optimal choice for the distribution key.
    - **Sort key**: `year`
    - **Reason**: `year` is often used in the WHERE clause, making it an efficient choice for the sort key.

- **artists_table**: 
    - **Distribution style**: KEY 
    - **Distribution key**: `artist_id`
    - **Reason**: `artist_id` is frequently used in join queries, but less often than `song_id`.
    - **Sort key**: `name`
    - **Reason**: `name` is often used in the WHERE clause, making it an efficient choice for the sort key.

- **time_table**: 
    - **Distribution style**: ALL
    - **Reason**: The time table is relatively small, so distributing it to all nodes ensures fast access.
    - **Sort key**: `start_time`
    - **Reason**: `start_time` is often used in the WHERE clause, making it an efficient choice for the sort key.

## create_tables.py

This script is used to create and drop tables in the Redshift cluster.

### Functions:
- `drop_tables(cur, conn)`: Drops tables if they exist.
- `create_tables(cur, conn)`: Creates tables.
- `main()`: Main function to drop and create tables.

# Start project
## How to run the project

To run the project, follow these steps in the given order:

### Prerequisites

1. **AWS account + user**: Ensure you have created a user with specified access permissions on AWS redshift + IAM portal.
2. **Python 3.x**: Make sure Python 3.x is installed on your machine.
3. **Dependencies**: Install the required Python packages using the following command:
    ```bash
    pip install -r requirements.txt
    ```

### Steps to run the project

1. **Configure the project**:
    - Update the `dwh.cfg` file with your AWS credentials, Redshift cluster configuration, IAM role details, and S3 bucket paths for data.

2. **Set up the infrastructure**:
    - Run the `create_infra.py` script to create the necessary IAM role and Redshift cluster.
    ```bash
    python create_infra.py
    ```

3. **Create tables**:
    - Run the `create_tables.py` script to create the staging and analytics tables in the Redshift cluster.
    ```bash
    python create_tables.py
    ```

4. **Run the ETL process**:
    - Run the `etl.py` script to extract data from S3, load it into staging tables in Redshift, and then insert the data into the analytics tables.
    ```bash
    python etl.py
    ```

5. **Clean up the infrastructure** (Optional):
    - If you want to delete the Redshift cluster and IAM role after completing the project, run the `delete_infra.py` script.
    ```bash
    python delete_infra.py
    ```

By following these steps, you will set up the data warehouse, load data into it, and be ready to perform analytics on the data.

# Example queries
After setting up the data warehouse and loading the data, you can run the following example queries to gain insights:

### Top 10 most played songs
```sql
SELECT s.title, COUNT(*) as play_count
FROM songplay sp
JOIN song s ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY play_count DESC
LIMIT 10;
```

### Number of users by subscription level
```sql
SELECT level, COUNT(*) as user_count
FROM users
GROUP BY level
ORDER BY user_count DESC;
```

### Daily song plays
```sql
SELECT t.start_time::date as play_date, COUNT(*) as play_count
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY play_date
ORDER BY play_date;
```

### Most active users
```sql
SELECT u.user_id, u.first_name, u.last_name, COUNT(*) as play_count
FROM songplays sp
JOIN users u ON sp.user_id = u.user_id
GROUP BY u.user_id, u.first_name, u.last_name
ORDER BY play_count DESC
LIMIT 10;
```

### Songs played by each user
```sql
SELECT u.user_id, u.first_name, u.last_name, s.title
FROM songplays sp
JOIN users u ON sp.user_id = u.user_id
JOIN songs s ON sp.song_id = s.song_id
ORDER BY u.user_id, s.title;
```

# README

The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository.
