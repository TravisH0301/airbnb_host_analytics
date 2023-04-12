WITH BASE AS (
    SELECT
        PARSE_DATE('%Y-%m-%d',  last_scraped) AS SNAPSHOT_DATE,
        CAST(
            RIGHT(
                listing_url,
                STRPOS(REVERSE(listing_url), '/') - 1
            ) AS INT64
        ) AS LISTING_ID,
        host_id AS HOST_ID,
        DATE_DIFF(
            CURRENT_DATE(),
            PARSE_DATE('%Y-%m-%d',  host_since),
            YEAR
        ) AS HOST_YEAR_OF_EXP,
        CAST(
            LENGTH(RTRIM(LTRIM(host_about))) - LENGTH(REPLACE(RTRIM(LTRIM(host_about)), ' ', '')) + 1 AS NUMERIC
        ) AS HOST_ABOUT_WORD_COUNT,
        host_is_superhost AS IS_HOST_SUPERHOST,
        host_has_profile_pic AS HAS_HOST_PROFILE_PHOTO,
        calculated_host_listings_count AS HOST_LISTING_COUNT,
        latitude AS LISTING_LATITUDE,
        longitude AS LISTING_LONGITUDE,
        CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS NUMERIC) AS LISTING_PRICE,
        PERCENTILE_CONT(CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS NUMERIC), 0.50) OVER() AS LISTING_PRICE_2Q,
        PERCENTILE_CONT(CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS NUMERIC), 0.75) OVER() * 1.5 AS LISTING_PRICE_UPPER,
        ROUND((30 - availability_30) / 30 * 100, 0) AS LISTING_OCCUPANCY_RATE,
        number_of_reviews AS LISTING_REVIEW_COUNT,
        neighbourhood_cleansed AS LISTING_MUNICIPALITY
    FROM {{ source('raw_data', 'airbnb_raw') }}
    WHERE
        1 = 1
        AND host_identity_verified = 't'
        AND availability_60 <> 0
        AND availability_90 <> 0
        AND availability_365 <> 0
        AND LOWER(neighbourhood_cleansed) IN (
            'banyule',
            'bayside',
            'boroondara',
            'brimbank',
            'cardinia',
            'casey',
            'darebin',
            'frankston',
            'glen eira',
            'greater dandenong',
            'hobsons bay',
            'hume',
            'kingston',
            'knox',
            'manningham',
            'maribyrnong',
            'maroondah',
            'melbourne',
            'melton',
            'monash',
            'moonee valley',
            'moreland',
            'mornington peninsula',
            'nillumbik',
            'port phillip',
            'stonnington',
            'whitehorse',
            'whittlesea',
            'wyndham',
            'yarra',
            'yarra ranges'
        )
        AND LOWER(room_type) = 'entire home/apt'
)
SELECT
    SNAPSHOT_DATE,
    LISTING_ID,
    HOST_ID,
    HOST_YEAR_OF_EXP,
    HOST_ABOUT_WORD_COUNT,
    IS_HOST_SUPERHOST,
    HAS_HOST_PROFILE_PHOTO,
    HOST_LISTING_COUNT,
    LISTING_LATITUDE,
    LISTING_LONGITUDE,
    LISTING_OCCUPANCY_RATE,
    LISTING_PRICE,
    LISTING_REVIEW_COUNT,
    LISTING_MUNICIPALITY,
    CURRENT_DATETIME() AS LOAD_DATETIME
FROM
    BASE
WHERE
    1 = 1
    AND LISTING_PRICE >= LISTING_PRICE_2Q
    AND LISTING_PRICE <= LISTING_PRICE_UPPER