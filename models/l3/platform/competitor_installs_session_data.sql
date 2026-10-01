{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : competitor_installs_session_data
-- Destination: analysis.competitor_installs_session_data  (unchanged)
-- Schedule   : 20 of month 00:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table analysis.competitor_installs_session_data
-- as

SELECT
  *
FROM (
  SELECT
    CAST(month AS date) AS month,
    company,
    store,
    country,
    country_code,
    region,
    device_type,
    SUM(visits) AS sessions,
    SUM(downloads) AS downloads,
    SUM(active_users) AS active_users
  FROM ( (
      SELECT
        month,
        company,
        null as store,
        country,
        country_code,
        region,
        device_type,
        visits,
        0 AS downloads,
        0 AS active_users
      FROM (
        WITH
          search_visits_distribution AS (
          SELECT
            site,
            MONTH,
            CASE
              WHEN channel LIKE '%Paid%' THEN 'Search: Paid'
            ELSE
            channel
          END
            AS channel,
            SUM(visits) AS visits
          FROM
            `SimilarWeb.search_visits_distribution*` # earliest date for search_visits_distribution is May 2019
          WHERE
            _TABLE_SUFFIX >= '20170101'
          GROUP BY
            site,
            MONTH,
            channel),
          static_analysis AS (
          SELECT
            site,
            MONTH,
            channel,
            channel_traffic AS visits
          FROM
            `SimilarWeb.static_analysis*`
          WHERE
            _TABLE_SUFFIX >= '20170101'
            AND channel != 'Search'),
          unioned_visits AS (
          SELECT
            *
          FROM
            search_visits_distribution
          UNION ALL
          SELECT
            *
          FROM
            static_analysis),
          visits_share AS (
          SELECT
            site,
            month,
            channel,
            SAFE_DIVIDE(visits,SUM(visits) OVER(PARTITION BY site, month)) AS SHARE
          FROM
            unioned_visits),
          geo AS (
          SELECT
            site,
            month,
            country,
            SUM(SHARE) AS SHARE
          FROM
            `SimilarWeb.geo*`
          WHERE
            _TABLE_SUFFIX BETWEEN '20170101'
            AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
          GROUP BY
            site,
            month,
            country),
          engagement AS (
          SELECT
            site,
            month,
            'Desktop' AS device_type,
            desk_visits AS visits
          FROM
            `SimilarWeb.traffic_engagement*`
          WHERE
            _TABLE_SUFFIX BETWEEN '20170101'
            AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
          UNION ALL
          SELECT
            site,
            month,
            'Mobile' AS device_type,
            mob_visits AS visits
          FROM
            `SimilarWeb.traffic_engagement*`
          WHERE
            _TABLE_SUFFIX BETWEEN '20170101'
            AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())),
          r AS # region
          (
          SELECT
            similarweb_country,
            country,
            region
          FROM
            `SimilarWeb.country_mapping`),
          web_alias AS (
          SELECT
            DOMAIN,
            company,
            company_type
          FROM
            `SimilarWeb.web_legend`
          UNION ALL
          SELECT
            DOMAIN,
            company,
            company_type
          FROM
            `SimilarWeb.web_legend_expired_domains`),
          post20190501_search_visits_distribution AS (
          SELECT
            engagement.month,
            engagement.device_type,
            web_alias.company,
            web_alias.company_type,
            visits_share.channel,
            r.country,
            r.region,
            CAST(SUM(geo.share*visits_share.share*engagement.visits) AS INT64) AS visits
          FROM
            engagement
          INNER JOIN
            geo
          ON
            engagement.site = geo.site
            AND engagement.month = geo.month
          INNER JOIN
            r
          ON
            geo.country = r.similarweb_country
          INNER JOIN
            visits_share
          ON
            engagement.site = visits_share.site
            AND engagement.month = visits_share.month
          INNER JOIN
            web_alias
          ON
            engagement.site = web_alias.domain
          GROUP BY
            month,
            device_type,
            company,
            company_type,
            channel,
            country,
            region),
          final_output AS (
          SELECT
            *
          FROM
            post20190501_search_visits_distribution
          WHERE
            company IN ( 'Skyscanner',
              'Trivago',
              'Momondo',
              'Saudia',
              'Swvl',
              'Aviasales',
              'Ryanair',
              'Wego',
              'Oyorooms',
              '99',
              'Lyft',
              'Azul',
              'Ixigo',
              'GOL Linhas Aéreas',
              'LATAM',
              'TripAdvisor',
              'American Airlines',
              'Dana Airlines',
              'Ethiopian Airlines',
              'Southwest',
              'Kayak',
              'Kulula',
              'Scoot',
              'Traveloka',
              'Trip.com',
              'Expedia',
              'FlySafair',
              'Booking.com',
              'Hotels.com',
              'HotelsCombined',
              'Google Flights',
              'Google Ads',
              'Flynas',
              'Flyadeal',
              'Flydubai',
              'Flyin',
              'IndiGo',
              'Almosafer',
              'Almatar',
              'Agoda',
              'Cleartrip',
              'Travelstart',
              'Airbnb',
              'Airblue',
              'Jeju Air',
              'Ctrip',
              'Hopper',
              'Vrbo',
              'Jinair',
              'FINN.no',
              'Rehlat',
              'MakeMyTrip',
              'MyRealTrip',
              'Mesafer',
              'Tathkarah',
              'Cheapflights',
              'Sastaticket',
              'Pakistan International Airlines',
              'Jazeera Airways',
              'Kuwait Airways',
              'bookme.pk',
              'Bookme.pk',
              'Alibaba.ir',
              'Safarmarket',
              'Eligasht',
              'Eghamat24',
              'Serene Air',
              'Flightio',
              'Ghasedak24',
              'Kojaro',
              'Cathay Pacific',
              'UNI Airways',
              'MrBilit',
              'Gulf Air',
              'Snapptrip',
              'Etihad Airways',
              'Enuygun',
              'GetYourGuide',
              'Thai Airways',
              'Cebu Pacific',
              'Etstur',
              'Odamax',
              'Jetstar',
              'Thai Lion Air',
              'VietJet Air',
              'Malindo Air',
              'Pegasus Airlines',
              'Vietnam Airlines',
              'Turkish Airlines',
              'Malaysia Airlines',
              'Singapore Airlines',
              'Philippine Airlines',
              'Ucuzabilet',
              'oBilet',
              'Klook',
              'eDreams',
              'EDreams',
              'Flight Centre',
              'Emirates',
              'Citilink',
              'EaseMyTrip',
              'Goibibo',
              'Trainline',
              'Ixigo',
              'Lionair',
              'Omio',
              'Kkday',
              'Marriott',
              'Yatra',
              'Pegipegi',
              'Tiket.com',
              'Via',
              'MyTour.vn',
              'Abay.vn',
              'Air New Zealand',
              'New Zealand Tourism',
              'Air Arabia',
              'Air France',
              'Vntrip',
              'Travelwings',
              'China Eastern Airlines',
              'EzTravel',
              'AsiaYo!',
              'Lion Travel',
              'Yahoo! Travel',
              'Wizz Air',
              'Mafengwo',
              'Qunar',
              'Elong',
              'Qyer',
              'Jalan',
              'Rakuten',
              'ForTravel.jp',
              'Ikyu',
              'JTB',
              'Interpark',
              'HomeAway',
              'Wotif',
              'Webjet',
              'Despegar',
              'Priceline',
              'Travelocity',
              'Grab',
              'Ola',
              'Gojek',
              'Delta',
              'Delta Air Lines',
              'Easyjet',
              'Oman Air',
              'RedDoorz',
              'Qatar Airways',
              'Qantas Airways',
              'Virgin Australia',
              'United Airlines',
              'Air India Express',
              'Air China',
              'Spring Airlines',
              'China Southern',
              'China Eastern',
              'Shenzhen Airlines',
              'All Nippon Airways',
              'Japan Airlines',
              'Taiwan Tourism',
              'Air India',
              'Egyptair',
              'British Airways',
              'SalamAir',
              'Singapore Tourism',
              'AirAsia',
              'Korean Air',
              'Asiana',
              'Hotelscan',
              'FindHotel',
              'Jetcost',
              'Neredekal',
              'Parvazhub',
              'Sepehr360',
              'Turismocity',
              'Viajacompara',
              'Viajala',
              'Bilet All',
              'eSky',
              'Etraveli',
              'FlightNetwork',
              'Fly365',
              'FlyBooking',
              'HappyEasyGo',
              'HolidayBazaar',
              'HolidayMe',
              'Kiwi',
              'Lastminute',
              'LINE TRAVEL JP',
              'Musafir',
              'MyHotelsSA',
              'Nereden Nereye',
              'NusaTrip',
              'Opodo',
              'Orbitz',
              'Otel.com',
              'Otelz',
              'Parvazyab',
              'Rumbo',
              'Seera',
              'SkySOUQ',
              'Skyticket',
              'SmartFares',
              'Tajawal',
              'Tatil Sepeti',
              'Tatil.com',
              'TatilBudur',
              'Travala',
              'TravelBook',
              'Tripsta',
              'Volagratis',
              'Wakanow',
              'Yamsafer',
              'Gathern',
              'Batoota',
              'Checkin.pk',
              'Findmyadventure.pk',
              'Jazz Mosafir',
              'Roomy',
              'Lokal',
              'Pakistan International Airlines',
              'Otelz',
              'Sastaticket',
              'Gathern'
              )
          UNION ALL
          SELECT
            *
          FROM
            SimilarWeb.pre20190501_search_visits_distribution)
        SELECT
          *
        FROM
          final_output) s
      LEFT JOIN (
        SELECT
          country_name,
          country_code
        FROM
          `wego-cloud.analytics.countries_misc`) c
      ON
        s.country = c.country_name)
    UNION ALL (
      SELECT
        CONCAT(month, "-01") AS month,
        company,
        store,
        country,
        country_code,
        region,
        null as device_type,
        0 AS visits,
        downloads,
        IFNULL(active_users, 0) AS active_users
      FROM (
        SELECT
          *
        FROM (
          SELECT
            apps.store AS store,
            apps.country AS country,
            country_mapping.country_abbreviation AS country_code,
            country_mapping.region AS region,
            apps.month AS month,
            app_id_legend.company AS company,
            app_id_legend.company_type AS company_type,
            CAST(SUM(apps.downloads) AS INT64) AS downloads,
            CAST(SUM(apps.active_users) AS INT64) AS active_users
          FROM (
            SELECT
              t3.month,
              t3.store,
              t3.app,
              t3.country,
              t3.app_id,
              t3.package_name,
              t4.active_users,
              t3.downloads
            FROM (
              SELECT
                FORMAT_TIMESTAMP('%Y-%m', CAST(LEFT(period, 10) AS date)) AS month,
                store,
                app,
                country,
                app_id,
                package_name,
                SUM(downloads) AS downloads
              FROM
                `appannie.compareapps*`
              GROUP BY
                month,
                store,
                app,
                app_id,
                package_name,
                country) AS t3
            FULL JOIN (
              SELECT
                FORMAT_TIMESTAMP('%Y-%m', date) AS month,
                store,
                app,
                country,
                app_id,
                package_name,
                SUM(active_users) active_users
              FROM
                `appannie.apps_usage*`
              GROUP BY
                month,
                store,
                app,
                app_id,
                package_name,
                country) AS t4
            ON
              t3.store = t4.store
              AND t3.app = t4.app
              AND t3.country = t4.country
              AND t3.app_id = t4.app_id
              AND t3.package_name = t4.package_name
              AND t3.month = t4.month) AS apps
          INNER JOIN (
            SELECT
              product_id,
              company,
              company_type
            FROM
              `wego-cloud.appannie.app_id_legend`) AS app_id_legend
          ON
            app_id_legend.product_id = apps.package_name
          LEFT JOIN (
            SELECT
              appannie_country,
              country_abbreviation,
              country,
              region
            FROM
              `wego-cloud.SimilarWeb.country_mapping`) AS country_mapping
          ON
            apps.country = country_mapping.appannie_country
          GROUP BY
            store,
            country,
            region,
            month,
            company,
            company_type,
            country_code
          HAVING
            downloads >= 0
          ORDER BY
            month DESC,
            downloads DESC)
        WHERE
          company != 'Paytm')
      WHERE
        country_code != "WW"
        AND company IN ( 'Skyscanner',
          'Trivago',
          'Momondo',
          'Saudia',
          'Swvl',
          'Aviasales',
          'Ryanair',
          'Wego',
          'Oyorooms',
          '99',
          'Lyft',
          'Azul',
          'Ixigo',
          'GOL Linhas Aéreas',
          'LATAM',
          'TripAdvisor',
          'American Airlines',
          'Dana Airlines',
          'Ethiopian Airlines',
          'Southwest',
          'Kayak',
          'Kulula',
          'Scoot',
          'Traveloka',
          'Trip.com',
          'Expedia',
          'FlySafair',
          'Booking.com',
          'Hotels.com',
          'HotelsCombined',
          'Google Flights',
          'Google Ads',
          'Flynas',
          'Flyadeal',
          'Flydubai',
          'Flyin',
          'IndiGo',
          'Almosafer',
          'Almatar',
          'Agoda',
          'Cleartrip',
          'Travelstart',
          'Airbnb',
          'Airblue',
          'Jeju Air',
          'Ctrip',
          'Hopper',
          'Vrbo',
          'Jinair',
          'FINN.no',
          'Rehlat',
          'MakeMyTrip',
          'MyRealTrip',
          'Mesafer',
          'Tathkarah',
          'Cheapflights',
          'Sastaticket',
          'Pakistan International Airlines',
          'Jazeera Airways',
          'Kuwait Airways',
          'bookme.pk',
          'Bookme.pk',
          'Alibaba.ir',
          'Safarmarket',
          'Eligasht',
          'Eghamat24',
          'Serene Air',
          'Flightio',
          'Ghasedak24',
          'Kojaro',
          'Cathay Pacific',
          'UNI Airways',
          'MrBilit',
          'Gulf Air',
          'Snapptrip',
          'Etihad Airways',
          'Enuygun',
          'GetYourGuide',
          'Thai Airways',
          'Cebu Pacific',
          'Etstur',
          'Odamax',
          'Jetstar',
          'Thai Lion Air',
          'VietJet Air',
          'Malindo Air',
          'Pegasus Airlines',
          'Vietnam Airlines',
          'Turkish Airlines',
          'Malaysia Airlines',
          'Singapore Airlines',
          'Philippine Airlines',
          'Ucuzabilet',
          'oBilet',
          'Klook',
          'eDreams',
          'EDreams',
          'Flight Centre',
          'Emirates',
          'Citilink',
          'EaseMyTrip',
          'Goibibo',
          'Trainline',
          'Ixigo',
          'Lionair',
          'Omio',
          'Kkday',
          'Marriott',
          'Yatra',
          'Pegipegi',
          'Tiket.com',
          'Via',
          'MyTour.vn',
          'Abay.vn',
          'Air New Zealand',
          'New Zealand Tourism',
          'Air Arabia',
          'Air France',
          'Vntrip',
          'Travelwings',
          'China Eastern Airlines',
          'EzTravel',
          'AsiaYo!',
          'Lion Travel',
          'Yahoo! Travel',
          'Wizz Air',
          'Mafengwo',
          'Qunar',
          'Elong',
          'Qyer',
          'Jalan',
          'Rakuten',
          'ForTravel.jp',
          'Ikyu',
          'JTB',
          'Interpark',
          'HomeAway',
          'Wotif',
          'Webjet',
          'Despegar',
          'Priceline',
          'Travelocity',
          'Grab',
          'Ola',
          'Gojek',
          'Delta',
          'Delta Air Lines',
          'Easyjet',
          'Oman Air',
          'RedDoorz',
          'Qatar Airways',
          'Qantas Airways',
          'Virgin Australia',
          'United Airlines',
          'Air India Express',
          'Air China',
          'Spring Airlines',
          'China Southern',
          'China Eastern',
          'Shenzhen Airlines',
          'All Nippon Airways',
          'Japan Airlines',
          'Taiwan Tourism',
          'Air India',
          'Egyptair',
          'British Airways',
          'SalamAir',
          'Singapore Tourism',
          'AirAsia',
          'Korean Air',
          'Asiana',
          'Hotelscan',
          'FindHotel',
          'Jetcost',
          'Neredekal',
          'Parvazhub',
          'Sepehr360',
          'Turismocity',
          'Viajacompara',
          'Viajala',
          'Bilet All',
          'eSky',
          'Etraveli',
          'FlightNetwork',
          'Fly365',
          'FlyBooking',
          'HappyEasyGo',
          'HolidayBazaar',
          'HolidayMe',
          'Kiwi',
          'Lastminute',
          'LINE TRAVEL JP',
          'Musafir',
          'MyHotelsSA',
          'Nereden Nereye',
          'NusaTrip',
          'Opodo',
          'Orbitz',
          'Otel.com',
          'Otelz',
          'Parvazyab',
          'Rumbo',
          'Seera',
          'SkySOUQ',
          'Skyticket',
          'SmartFares',
          'Tajawal',
          'Tatil Sepeti',
          'Tatil.com',
          'TatilBudur',
          'Travala',
          'TravelBook',
          'Tripsta',
          'Volagratis',
          'Wakanow',
          'Yamsafer',
          'Gathern',
          'Batoota',
          'Checkin.pk',
          'Findmyadventure.pk',
          'Jazz Mosafir',
          'Roomy',
          'Lokal',
          'Pakistan International Airlines',
          'Otelz',
          'Sastaticket',
          'Gathern')) )
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
  ORDER BY
    1) t1
LEFT JOIN (
  SELECT
    DISTINCT Brand,
    BrandType
  FROM
    `SimilarWeb.outgoing_referrals_mapping`) t2
ON
  t1.company = t2.brand
{% endraw %}
