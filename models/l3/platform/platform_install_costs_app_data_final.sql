{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : platform_install_costs_app_data_final
-- Destination: analysis.platform_install_costs_app_data_final  (unchanged)
-- Schedule   : every day 03:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table 
-- analysis.platform_install_costs_app_data_final
-- partition by date(date)
-- as

SELECT
      case
      when lower(Campaign) like "%-rt-%" then "retargeting"
      when lower(Campaign) like "asa-%" then "apple search ads"
      when lower(Campaign) like "%-asa-%" then "apple search ads"
      when lower(Campaign) like "google-%" then "google"
      when lower(Campaign) like "fb-%" then "facebook"
      when lower(Campaign) like "facebook-%" then "facebook"
      when lower(Campaign) like "tiktok-%" then "tiktok"
      when lower(Campaign) like "twitter-%" then "twitter"
      when lower(Campaign) like "snapchat-%" then "snapchat"
      when Campaign is null then null
      else Campaign end as channel,
      case
      when lower(Campaign) like "%-sa-%" then "SA"
      when lower(Campaign) like "%-ksa-%" then "SA"
      when lower(Campaign) like "%_ksa_%" then "SA"
      when lower(Campaign) like "%-ae-%" then "AE"
      when lower(Campaign) like "%-uae-%" then "AE"
      when lower(Campaign) like "%-kw-%" then "KW"
      when lower(Campaign) like "%_kw_%" then "KW"
      when lower(Campaign) like "%-om-%" then "OM"
      when lower(Campaign) like "%-bh-%" then "BH"
      when lower(Campaign) like "%-qa-%" then "QA"
      when lower(Campaign) like "%-eg-%" then "EG"
      when lower(Campaign) like "%-in-%" then "IN"
      when lower(Campaign) like "%-pk-%" then "PK"
      when lower(Campaign) like "%-jo-%" then "JO"
      when lower(Campaign) like "%-row-%" then "ROW"
      else "ROW" end as market,
      case
      when lower(Campaign) like "%-en-%" then "en"
      when lower(Campaign) like "%- en -%" then "en"
      when Campaign like "%-EN" then "en"
      when Campaign like "%-EN(%" then "en"
      when Campaign like "%-EN %" then "en"
      when lower(Campaign) like "%english%" then "en"
      when lower(Campaign) like "%-ar-%" then "ar"
      when lower(Campaign) like "%- ar -%" then "ar"
      when Campaign like "%-AR" then "ar"
      when Campaign like "%-AR(%" then "ar"
      when Campaign like "%-AR %" then "ar"
      when lower(Campaign) like "%arabic%" then "ar"
      when Campaign is null then null
      else "generic" end as locale,
      case
      when lower(Campaign) like "%tourism%" then "nto"
      when lower(Campaign) like "%flight%" then "flights"
      when lower(Campaign) like "%hotel%" then "hotels"
      when lower(Campaign) like "%brand%" then "brand"
      when lower(Campaign) like "%youtube%" then "brand"
      when Campaign is null then null
      else "generic" end as product,
      case
      when lower(Campaign) like "%-ios-%" then "ios"
      when Campaign like "%iOS%" then "ios"
      when Campaign like "%IOS%" then "ios"
      when lower(Campaign) like "%-android-%" then "android"
      when lower(Campaign) like "%-and-%" then "android"
      when Campaign like "%-AND%" then "android"
      when Campaign like "%ASA%" then "ios"
      else "generic" end as device,
      case
      when lower(Campaign) like "%meta%" then "meta"
      when lower(Campaign) like "%bow%" then "bow"
      when Campaign is null then null
      else "generic" end as model,
      cast(Date as timestamp) as date,
      Cost as cost,
      Installs as installs,
      Campaign as campaign
      FROM
       (select * from  `wego-cloud.analysis.platform_install_costs_app_data`
       union all 
       select * from `wego-cloud.analysis.platform_install_costs_app_data_v2`)
{% endraw %}
