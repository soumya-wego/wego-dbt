{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotels_searches_funnel_p1
-- Destination: analysis.hotels_searches_funnel  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
DELETE FROM `wego-cloud.analysis.hotels_searches_funnel`
WHERE DATE(created_at) = DATE_SUB(@run_date, INTERVAL 1 DAY);

INSERT INTO `wego-cloud.analysis.hotels_searches_funnel`
WITH hotels_searches AS (
  SELECT
    search_id, pageview_id, created_at, client_id, session_id, device_type, site_code,
    currency_code, hotel_id, district_id, city_code, region_id, country_code,
    check_in, check_out, rooms_count, guests_count, locale,
  FROM `wego-cloud.wego_analytics.hotels_searches`
  WHERE DATE(_PARTITIONTIME) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
, sessions AS (
  SELECT
    session_id,
    wg_campaign,
    REGEXP_EXTRACT(landing_url, r'[?&]wg_internal_campaign=([^&]+)') AS wg_internal_campaign,
    REGEXP_EXTRACT(landing_url, r'[?&]wg_internal_source=([^&]+)')   AS wg_internal_source
  FROM `wego-cloud.wego_analytics.sessions`
  WHERE DATE(_PARTITIONTIME) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
, hotel_details AS (
  SELECT created_at, client_id, session_id, event_id, former_event_id, page_type, page_url,
  '2. hotel detail page' funnel_step, latter_event_id
  FROM (
    SELECT *, _PARTITIONTIME, LEAD(EVENT_ID) OVER (PARTITION BY session_id ORDER BY created_at) AS latter_event_id
    FROM `wego-cloud.wego_analytics.pageviews`
    WHERE DATE(_PARTITIONTIME) = DATE_SUB(@run_date, INTERVAL 1 DAY)
  )
  WHERE page_type LIKE 'hotels_detail_page_%'
  AND DATE(_PARTITIONTIME) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
, room_details AS (
  SELECT created_at, client_id, session_id, event_id, former_event_id, page_type,
  '3. room detail page' funnel_step
  FROM `wego-cloud.wego_analytics.pageviews`
  WHERE page_type LIKE 'hotels_room_details%'
  AND DATE(_PARTITIONTIME) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
, hotels_booking AS (
  SELECT created_at, client_id, session_id, event_id, former_event_id, page_type,
  '4. booking page' funnel_step
  FROM `wego-cloud.wego_analytics.pageviews`
  WHERE page_type = 'hotels_booking'
  AND DATE(_PARTITIONTIME) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
, payment AS (
  SELECT created_at, client_id, session_id, event_id, former_event_id, page_type,
  '5. payment page' funnel_step
  FROM `wego-cloud.analysis.wego_pageviews_analysis`
  WHERE page_type = 'hotels_booking'
  AND (
    (event_object = 'page_cta' AND event_action = 'make payment'
     AND device_type IN ('android-app', 'ios-app'))
    OR
    (event_object = 'payment_confirmation' AND event_action = 'submit'
     AND device_type NOT IN ('android-app', 'ios-app'))
  )
  AND DATE(created_at) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
, paid_booking AS (
  SELECT
    session_id,
    MIN(created_at) AS created_at
  FROM `wego-cloud.wego_analytics.hotels_bookings`
  WHERE conversions_adjusted > 0
  AND DATE(created_at) >= DATE_SUB(@run_date, INTERVAL 30 DAY)
  GROUP BY session_id
)
, combine2 AS (
  SELECT
    hs.search_id, hs.pageview_id, hs.created_at, hs.client_id, hs.session_id,
    hs.device_type, hs.site_code, hs.currency_code, hs.hotel_id, hs.district_id,
    hs.city_code, hs.region_id, hs.country_code, hs.check_in, hs.check_out,
    hs.rooms_count, hs.guests_count,
    CASE WHEN hd.session_id IS NOT NULL THEN 1 ELSE 0 END AS hotel_detail_page_flag,
    CASE WHEN rd.session_id IS NOT NULL THEN 1 ELSE 0 END AS room_detail_page_flag,
    CASE WHEN hb.session_id IS NOT NULL THEN 1 ELSE 0 END AS hotels_booking_page_flag,
    CASE WHEN pay.session_id IS NOT NULL THEN 1 ELSE 0 END AS payment_page_flag,
    CASE WHEN paid.session_id IS NOT NULL THEN 1 ELSE 0 END AS paid_flag,
    paid.created_at AS booking_time,
    NULL AS booking_count,
    MAX(hs.created_at) OVER (PARTITION BY hs.session_id) AS latest_search,
    COUNT(DISTINCT hs.search_id) OVER (PARTITION BY hs.session_id) AS search_count,
    hs.locale,
    s.wg_campaign,
    s.wg_internal_campaign,
    s.wg_internal_source
  FROM hotels_searches AS hs
    LEFT JOIN sessions AS s        ON hs.session_id = s.session_id
    LEFT JOIN hotel_details AS hd  ON hs.session_id = hd.session_id AND hs.client_id = hd.client_id
    LEFT JOIN room_details AS rd   ON hs.session_id = rd.session_id AND hs.client_id = rd.client_id
    LEFT JOIN hotels_booking AS hb ON hs.session_id = hb.session_id AND hs.client_id = hb.client_id
    LEFT JOIN payment AS pay       ON hs.session_id = pay.session_id AND hs.client_id = pay.client_id
    LEFT JOIN paid_booking AS paid ON hs.session_id = paid.session_id
  GROUP BY ALL
)
SELECT
  search_id, pageview_id, created_at, client_id, session_id, device_type, site_code,
  currency_code, hotel_id, district_id, city_code, region_id, country_code,
  check_in, check_out, rooms_count, guests_count,
  hotel_detail_page_flag, room_detail_page_flag, hotels_booking_page_flag,
  payment_page_flag, paid_flag, booking_time, booking_count,
  latest_search, search_count,
  CASE WHEN latest_search = created_at THEN 1 ELSE 0 END AS latest_search_flag,
  locale,
  wg_campaign,
  wg_internal_campaign,
  wg_internal_source
FROM combine2
{% endraw %}
