{{ config(materialized='view') }}

SELECT 
    review_id, 
    order_id, 
    cast(review_score as int) as review_score, 
    review_comment_title, 
    review_comment_message, 
    cast(review_creation_date as timestamp) as review_creation_date, 
    cast(review_answer_timestamp as timestamp) as review_answer_timestamp,
    cast(updated_at_timestamp as timestamp) as updated_at_timestamp 
 
FROM 
    project_landing.order_reviews_ayahany
