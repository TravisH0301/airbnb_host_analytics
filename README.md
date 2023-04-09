# airbnb_host_analytics

Effect of host characteristics on listing performance (occupancy rate)
- multi-listing vs. single-listing: vertical bar chart
(avg occupancy rate for multi listing)
calculated_host_listings_count

- host experience: horizontal bar chart
(0, 1-4, 4-7, 7-10, >10)
host_since (just use year only)

- profile pic

- super host 
(>4.8 review rating, >90% response rate, completed total of 100 nights, <1% cancellation rate over the past 12 months)

- number of reviews 
(0, 1-10, 10-50, 50-100, >100)
number_of_reviews

- host about
(descriptive vs non-descriptive)
(use quantiles)

Condition:
- use availability_30 for occupancy rate: availability_60, availability_90 & availability_365 must not be 0
- municipality: Melbourne
- verified host 
- price within 25th - 75th quantiles to reduce price factor
- room type: entire home/apt

Source: http://insideairbnb.com/get-the-data/

Data Modelling:
- fact_host_listing
id, listing_id, host_id, occupancy_rate, snapshot_date (14/03/2023)

- dim_host_character
host_id, is_super_host, has_profile_photo, experience_year, listing_count, host_about_word_count, is_identity_verified
, total_review_count

- dim_listing
listing_id, municipality, latitude, longitude, room_type, price
