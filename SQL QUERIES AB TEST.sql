-- 1) Calculating Overall Primary and Secondary signup rate for control and variant groups


-- Overall Primary and Secondary signup rate for control and variant groups

SELECT group_name, count(DISTINCT ee.user_id) AS total_users,

COUNT(DISTINCT CASE WHEN event_type = 'signup_completed' THEN ue.user_id END) AS signup_completed_count, -- counting users who signed up

COUNT(DISTINCT CASE WHEN event_type LIKE '%signup%' THEN ue.user_id END) AS signup_started_count, -- counting users who started the signup

COUNT(DISTINCT CASE WHEN event_type = 'signup_completed' THEN ue.user_id END)::FLOAT /  -- calculating primary signup rate ((signups/total_users)*100)
COUNT(DISTINCT EE.USER_ID) * 100 AS primary_signup_rate, 

COUNT(DISTINCT CASE WHEN event_type = 'signup_completed' THEN ue.user_id END)::FLOAT / -- calculating secondary signup rate((signups/signusps_started)*100) 
COUNT(DISTINCT CASE WHEN event_type LIKE '%signup%' THEN ue.user_id END)::FLOAT * 100 AS secondary_signup_rate

FROM experiment_exposures AS ee -- using experiment_exposures to get users who were exposed to the test
JOIN experiment_groups AS eg USING (group_id) -- joining experiment_groups to get group_id and group_names (control, variant)
JOIN user_events AS ue USING(user_id) -- joining user_events to access user events and activity information

WHERE exposure_type = 'element_visible' -- only focusing and element visible exposure type as "page render" doesn't guarantee if user viewed the page or not

GROUP BY group_name -- grouped by group_name (control and variant) for accurate calculations




-- 2) Calculating the avg time taken for signup in variant and control groups:


-- Average Time Taken for signup across variant and control group

WITH signup_filter AS ( -- In signup_filter CTE, we fetched users who either completed or started the signup process

SELECT user_id, event_type, event_date
FROM user_events AS ue 
JOIN experiment_exposures AS ee USING (user_id)
JOIN experiment_groups AS eg USING (group_id)
WHERE event_type IN ('signup_completed', 'signup_started')), 

filtered_users AS ( -- In filtered_users, we are filering those users who either just started the signup (didn't complete it) or directly signedup(may be from a referral link)
SELECT user_id, event_type, event_date, 
LEAD(event_type) OVER(PARTITION BY USER_ID) AS next_event, -- fetching next event for each user_id
LEAD(event_date) OVER(PARTITION BY USER_ID) AS next_date -- fetching next event_date for each user_id

FROM signup_filter 

WHERE user_id NOT IN -- Here, we are excluding users who either just started the signup or signedup directly

(SELECT user_id
FROM user_events AS ue 
JOIN experiment_exposures AS ee USING (user_id)
JOIN experiment_groups AS eg USING (group_id)
WHERE event_type IN ('signup_started','signup_completed')
GROUP BY user_id
HAVING count(event_id) = 1)

ORDER BY user_id, event_type DESC), -- ordering by user and event type (descending) so that signup_started is first event


signup_difference AS ( -- In signup_difference, we calculate signup time difference between users
SELECT user_id, (next_date - event_date) AS signup_time
FROM filtered_users)

SELECT group_name, avg(signup_time) AS avg_signup_time -- Here, we calculated avg_signup_time for each group
FROM signup_difference AS c 
JOIN experiment_exposures AS ee USING (user_id)
JOIN experiment_groups AS eg USING (group_id)
GROUP BY group_name



-- 3) Calculating primary and secondary signup rate across countries:



-- Primary and Secondary signup rate across countries

SELECT group_name, country, count(DISTINCT ee.user_id) AS total_users,

COUNT(DISTINCT CASE WHEN event_type = 'signup_completed' THEN ue.user_id END) AS signup_completed_count, -- counting users who signed up

COUNT(DISTINCT CASE WHEN event_type LIKE '%signup%' THEN ue.user_id END) AS signup_started_count, -- counting users who started the signup

COUNT(DISTINCT CASE WHEN event_type = 'signup_completed' THEN ue.user_id END)::FLOAT / -- calculating primary signup rate ((signups/total_users)*100)
COUNT(DISTINCT EE.USER_ID) * 100 AS primary_signup_rate, 

COUNT(DISTINCT CASE WHEN event_type = 'signup_completed' THEN ue.user_id END)::FLOAT / -- calculating secondary signup rate((signups/signusps_started)*100)
COUNT(DISTINCT CASE WHEN event_type LIKE '%signup%' THEN ue.user_id END)::FLOAT * 100 AS secondary_signup_rate

FROM experiment_exposures AS ee -- using experiment_exposures to get users who were exposed to the test
JOIN experiment_groups AS eg USING (group_id) -- joining experiment_groups to get group_id and group_names (control, variant)
JOIN user_events AS ue USING(user_id) -- joining user_events to access user events and activity information
JOIN users AS uu USING (user_id) -- joining users to get demographic information of users

WHERE exposure_type = 'element_visible' -- only focusing and element visible exposure type as "page render" doesn't guarantee if user viewed the page or not

GROUP BY group_name, country -- grouped by group_name (control and variant) and country for accurate calculations

ORDER BY country



-- 4) Calculating primary and secondary metric across device type:


-- Primary and Secondary signup rate across device types

SELECT group_name, device_type, count(DISTINCT ee.user_id) AS total_users, 

COUNT(DISTINCT CASE WHEN event_type = 'signup_completed' THEN ue.user_id END) AS signup_completed_count, -- counting users who signed up

COUNT(DISTINCT CASE WHEN event_type LIKE '%signup%' THEN ue.user_id END) AS signup_started_count, -- counting users who started the signup

COUNT(DISTINCT CASE WHEN event_type = 'signup_completed' THEN ue.user_id END)::FLOAT / -- calculating primary signup rate ((signups/total_users)*100)
COUNT(DISTINCT EE.USER_ID) * 100 AS primary_signup_rate, 

COUNT(DISTINCT CASE WHEN event_type = 'signup_completed' THEN ue.user_id END)::FLOAT / -- calculating secondary signup rate((signups/signusps_started)*100)
COUNT(DISTINCT CASE WHEN event_type LIKE '%signup%' THEN ue.user_id END)::FLOAT * 100 AS secondary_signup_rate

FROM experiment_exposures AS ee -- using experiment_exposures to get users who were exposed to the test
JOIN experiment_groups AS eg USING (group_id) -- joining experiment_groups to get group_id and group_names (control, variant)
JOIN user_events AS ue USING(user_id) -- joining user_events to access user events and activity information
JOIN users AS uu USING (user_id) -- joining users to get demographic information of users

WHERE exposure_type = 'element_visible' -- only focusing and element visible exposure type as "page render" doesn't guarantee if user viewed the page or not

GROUP BY group_name, device_type -- grouped by group_name (control and variant) and device_type for accurate calculations

ORDER BY device_type



-- BELOW ARE THE VIEWS CREATED FOR DASHBOARD BUILDING:

-- 1) MATERIALIZED VIEW FOR FCT SIGNUP RATES:

CREATE MATERIALIZED VIEW fct_signup_rate AS ( -- This calculated signup rates based on combination of group_id, country, and device_type to be used with slicers in dashboard

SELECT group_id, country, device_type, count(DISTINCT ee.user_id) AS total_users,
 
COUNT(DISTINCT CASE WHEN event_type = 'signup_completed' THEN ue.user_id END) AS signup_completed_count, 

COUNT(DISTINCT CASE WHEN event_type LIKE '%signup%' THEN ue.user_id END) AS signup_started_completed_count


FROM experiment_exposures AS ee 
JOIN user_events AS ue USING(user_id)
JOIN users AS uu USING (user_id)

WHERE exposure_type = 'element_visible'

GROUP BY group_id, country, device_type)



-- 2) MATERIALIZED VIEW for signups by date, country, and device type:


CREATE MATERIALIZED VIEW fct_signups AS ( -- It calculates signups across date, groups, country, and device

SELECT signup_date, ee.group_id, group_name, country, device_type, count(DISTINCT user_id) AS user_count
FROM experiment_exposures AS ee 
JOIN experiment_groups AS eg USING (experiment_id)
JOIN user_events AS ue USING (user_id)
JOIN users AS us USING (user_id)
WHERE event_type = 'signup_completed'
GROUP BY signup_date, ee.group_id, group_name, country, device_type)


-- 3) MATERIALIZED VIEW for device type dimension:

CREATE MATERIALIZED VIEW dim_device_type AS ( -- It is a dimension to connect fact tables with device_type for effective dynamic slicing
SELECT DISTINCT device_type
FROM users)



-- 4) MATERIALIZED VIEW FOR country dimension:

CREATE MATERIALIZED VIEW dim_country AS ( -- It is a dimension to connect fact tables with country for effective dynamic slicing
SELECT DISTINCT country
FROM users)


-- 5) MATERIALIZED VIEW FOR groups dimension:

CREATE MATERIALIZED VIEW dim_groups as ( -- It is a dimension to connect fact tables with groups for effective dynamic slicing
SELECT DISTINCT group_id, group_name, group_percentage
FROM experiment_groups
)