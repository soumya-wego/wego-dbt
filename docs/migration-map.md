# Migration map — scheduled queries → dbt models

Generated from the live scheduled-query inventory (enabled queries only; the 93
disabled ones are retire candidates, not migrations). Table names stay exactly as
they are today; the model file name = the destination table name. Layer rule used:
a destination that other scheduled queries READ is a building block -> `l2/`;
otherwise it serves one consumer -> `l3/`. (The planned raw-cleaning builds —
hotels rates, flights fares — are the only future `l1/` models.) Every row is a
proposal; the owner confirms layer and group at migration time.

| # | Scheduled query | Destination (unchanged) | Proposed model path | State |
|---|---|---|---|---|
| 1 | Flights Bookings Ticket Level | `wego_analytics.flights_bookings_ticket_level` | `models/l2/flights/flights_bookings_ticket_level.sql` |  |
| 2 | Unused_Tickets_2026_Mapped sync | `integrated_bookings_flights.unused_tickets` | `models/l2/flights/unused_tickets.sql` |  |
| 3 | bow_branded_fares_selected | `analysis.bow_branded_fares_selected` | `models/l2/flights/bow_branded_fares_selected.sql` |  |
| 4 | flights_amadeus_schedule | `analysis.flights_amadeus_schedule` | `models/l2/flights/flights_amadeus_schedule.sql` |  |
| 5 | flights_bookings_passengers_daily_run | `wego_analytics.flights_bookings_passengers` | `models/l2/flights/flights_bookings_passengers.sql` |  |
| 6 | ipcc_candidate_vs_chosen | `analysis.ipcc_candidate_vs_chosen` | `models/l2/flights/ipcc_candidate_vs_chosen.sql` |  |
| 7 | ipcc_checker_reference | `analysis.ipcc_checker_reference` | `models/l2/flights/ipcc_checker_reference.sql` |  |
| 8 | ipcc_scorecard_30d | `analysis.ipcc_scorecard_30d` | `models/l2/flights/ipcc_scorecard_30d.sql` |  |
| 9 | pricing_fares_analysis_daily_append | `analysis.pricing_fares_analysis` | `models/l2/flights/pricing_fares_analysis.sql` |  |
| 10 | bowh_supplier_availability_aggregated | `analysis.bowh_supplier_availability_aggregated` | `models/l2/hotels/bowh_supplier_availability_aggregated.sql` |  |
| 11 | hotels_partner_request_responses_daily_append | `analysis.hotels_partner_requests_responses` | `models/l2/hotels/hotels_partner_requests_responses.sql` |  |
| 12 | hotels_partner_requests_responses_aggregated_append | `analysis.hotels_partner_requests_responses_aggregated` | `models/l2/hotels/hotels_partner_requests_responses_aggregated.sql` |  |
| 13 | hotels_pricing_engine_distribution | `pricing_engine.hotels_pricing_engine_distribution` | `models/l2/hotels/hotels_pricing_engine_distribution.sql` |  |
| 14 | hotels_pricing_engine_distribution_calculation | `pricing_engine.hotels_pricing_engine_distribution_calculation` | `models/l2/hotels/hotels_pricing_engine_distribution_calculation.sql` |  |
| 15 | hotels_pricing_engine_ota_calculation | `pricing_engine.hotels_pricing_engine_ota_calculation` | `models/l2/hotels/hotels_pricing_engine_ota_calculation.sql` |  |
| 16 | hotels_searches_funnel_p1 | `analysis.hotels_searches_funnel` | `models/l2/hotels/hotels_searches_funnel.sql` |  |
| 17 | hotels_searches_funnel_p2 | `analysis.hotels_searches_funnel` | `models/l2/hotels/hotels_searches_funnel.sql` |  |
| 18 | saba_cross_supplier_netcost | `analysis.saba_cross_supplier_netcost` | `models/l2/hotels/saba_cross_supplier_netcost.sql` |  |
| 19 | wego_hotels_rates_analysis | `analysis.wego_rates_analysis` | `models/l2/hotels/wego_rates_analysis.sql` |  |
| 20 | ab testing | `wego_analytics.ab_testing` | `models/l2/other/ab_testing.sql` |  |
| 21 | BoWF_supplier_fare_analysis | `analysis.bow_flights_supplier_fare_analysis` | `models/l2/platform/bow_flights_supplier_fare_analysis.sql` |  |
| 22 | bow_gmv_cogs_flights_hotels_pivoted | `aaaaa_temporary_export_folder.gmv_cogs_flights_hotels_unpivoted` | `models/l2/platform/gmv_cogs_flights_hotels_unpivoted.sql` |  |
| 23 | campaign_id_recent_name_mapping | `analysis.campaign_id_recent_name_mapping` | `models/l2/platform/campaign_id_recent_name_mapping.sql` |  |
| 24 | client_lifetime_kpi_delsert | `analysis.client_lifetime_kpis` | `models/l2/platform/client_lifetime_kpis.sql` |  |
| 25 | client_session_aggregated_delsert | `analysis.clients_sessions_aggregated_daily` | `models/l2/platform/clients_sessions_aggregated_daily.sql` |  |
| 26 | clients_sessions_aggregated_daily_master_delsert | `analysis.clients_sessions_aggregated_daily_master` | `models/l2/platform/clients_sessions_aggregated_daily_master.sql` |  |
| 27 | wego_pageviews_analysis_daily_append | `analysis.wego_pageviews_analysis` | `models/l2/platform/wego_pageviews_analysis.sql` |  |
| 28 | skyscanner_bid_hotel_list | `analysis.skyscanner_bid_hotel_list` | `models/l2/skyscanner/skyscanner_bid_hotel_list.sql` |  |
| 29 | Amadeus_Schedule-Routes | `Flights_Routes.amadeus_schedule-routes` | `models/l3/flights/amadeus_schedule-routes.sql` | FAILING |
| 30 | Flights Bookings Offline | `wego_analytics.flights_bookings_offline` | `models/l3/flights/flights_bookings_offline.sql` |  |
| 31 | Flights Bookings Unused Tickets Logs Audit | `aaaaa_temporary_export_folder.flights_bookings_unused_ticket_logs_audit_0` | `models/l3/flights/flights_bookings_unused_ticket_logs_audit_0.sql` |  |
| 32 | Offline flights bookings - Back Office dataset | `wego_analytics.flights_bookings_offline_back_office` | `models/l3/flights/flights_bookings_offline_back_office.sql` |  |
| 33 | autopricing_ab_test | `analysis.autopricing_ab_test` | `models/l3/flights/autopricing_ab_test.sql` | MIGRATED (pilot) |
| 34 | bow_flights_bookings_netsuite_details_full | `aaaaa_temporary_export_folder.gmv_flights_booking_v2_1` | `models/l3/flights/gmv_flights_booking_v2_1.sql` |  |
| 35 | bow_flights_search_requests_daily_append | `analysis.bow_flights_search_requests` | `models/l3/flights/bow_flights_search_requests.sql` |  |
| 36 | fare_family_price_analysis | `analysis.fare_family_price_analysis` | `models/l3/flights/fare_family_price_analysis.sql` |  |
| 37 | flight_sort_order_bqml | `flight_sort_order_ml.data_date_range_` | `models/l3/flights/data_date_range_.sql` | FAILING |
| 38 | flights_clicks_price_accuracy_check_daily_append | `analysis.flights_clicks_price_accuracy_check` | `models/l3/flights/flights_clicks_price_accuracy_check.sql` |  |
| 39 | flights_impressions_logs_daily_append | `wego_analytics.flights_searches_details_impressions_logs` | `models/l3/flights/flights_searches_details_impressions_logs.sql` | FAILING |
| 40 | flights_impressions_logs_daily_append_clustered | `wego_analytics.flights_searches_details_impressions_logs_clustered` | `models/l3/flights/flights_searches_details_impressions_logs_clustered.sql` | FAILING |
| 41 | flights_partner_requests_responses_daily_append | `analysis.flights_partner_requests_responses` | `models/l3/flights/flights_partner_requests_responses.sql` |  |
| 42 | flights_price_trends_history | `flights_price_guidance.min_price_history` | `models/l3/flights/min_price_history.sql` | FAILING |
| 43 | flights_rate_cache_ttl | `flights_cache_analysis.ttl_strategy` | `models/l3/flights/ttl_strategy.sql` | FAILING |
| 44 | flights_searches_fare_cache_analysis | `analysis.flights_searches_fare_cache_analysis` | `models/l3/flights/flights_searches_fare_cache_analysis.sql` |  |
| 45 | flights_searches_isa_polling | `analysis.flights_searches_isa_polling` | `models/l3/flights/flights_searches_isa_polling.sql` |  |
| 46 | integrated_bookings_flights_booking_margins_vendor_commissions_dedup | `integrated_bookings_flights.booking_margins` | `models/l3/flights/booking_margins.sql` | FAILING |
| 47 | integrated_bookings_flights_booking_margins_vendor_commissions_dedup2 | `integrated_bookings_flights.booking_margins` | `models/l3/flights/booking_margins.sql` | FAILING |
| 48 | ipcc_performance_evaluation | `analysis.ipcc_performance_evaluation` | `models/l3/flights/ipcc_performance_evaluation.sql` |  |
| 49 | ipcc_top5_selection | `analysis.ipcc_top5_selection` | `models/l3/flights/ipcc_top5_selection.sql` |  |
| 50 | temp_autopricing_bookings_variant | `analysis.temp_autopricing_bookings_variant` | `models/l3/flights/temp_autopricing_bookings_variant.sql` |  |
| 51 | wegopro_flights_bookings | `analysis.wegopro_flights_bookings` | `models/l3/flights/wegopro_flights_bookings.sql` | FAILING |
| 52 | Hotels_display_price_matching_insert_job | `analysis.hotels_display_price_matching_master` | `models/l3/hotels/hotels_display_price_matching_master.sql` |  |
| 53 | Marketing_Feeds_Free_Cancellation_Hotels_List | `marketing_feeds.temp_free_cancellation_hotels` | `models/l3/hotels/temp_free_cancellation_hotels.sql` |  |
| 54 | RTP_analysis_aggregated_daily_append | `analysis.hotels_rtp_analysis_aggregated_daily` | `models/l3/hotels/hotels_rtp_analysis_aggregated_daily.sql` | FAILING |
| 55 | bow_hotels_bookings_netsuite_details_full | `aaaaa_temporary_export_folder.gmv_hotels_booking_v1_1` | `models/l3/hotels/gmv_hotels_booking_v1_1.sql` |  |
| 56 | bow_supplier_hotel_requests_daily_append | `analysis.bow_supplier_hotel_requests` | `models/l3/hotels/bow_supplier_hotel_requests.sql` |  |
| 57 | bow_supplier_hotel_searches_daily_append | `analysis.bow_supplier_hotel_searches` | `models/l3/hotels/bow_supplier_hotel_searches.sql` |  |
| 58 | bow_supplier_hotels_price_check_daily_append | `analysis.bow_supplier_hotel_price_check` | `models/l3/hotels/bow_supplier_hotel_price_check.sql` |  |
| 59 | bow_trivago_partner_report | `analysis.bow_trivago_partner_report` | `models/l3/hotels/bow_trivago_partner_report.sql` |  |
| 60 | bowh_supplier_channel_availability_stats_append | `analysis.bowh_supplier_channel_availability_stats` | `models/l3/hotels/bowh_supplier_channel_availability_stats.sql` |  |
| 61 | goreward_coverage_daily | `analysis.goreward_coverage_daily` | `models/l3/hotels/goreward_coverage_daily.sql` |  |
| 62 | goreward_hotel_list_v1 | `analysis.goreward_comparable_v1` | `models/l3/hotels/goreward_comparable_v1.sql` |  |
| 63 | hotel_matching_overall_daily_run | `analysis.hotel_matching_overall` | `models/l3/hotels/hotel_matching_overall.sql` |  |
| 64 | hotel_matching_provider_daily_run | `analysis.hotel_matching_provider` | `models/l3/hotels/hotel_matching_provider.sql` |  |
| 65 | hotel_sort_order_bqml | `hotel_sort_order_ml.data_date_range_` | `models/l3/hotels/data_date_range_.sql` |  |
| 66 | hotel_sort_order_bqml_bow | `hotel_sort_order_ml.bow_data_date_range_` | `models/l3/hotels/bow_data_date_range_.sql` |  |
| 67 | hotel_wego_rates_room_detail_daily_append | `analysis.hotel_wego_rates_room_detail` | `models/l3/hotels/hotel_wego_rates_room_detail.sql` |  |
| 68 | hotels_bow_rate_cache_ttl_metrics | `hotels_bow_rates_analysis.ttl_strategy_metrics` | `models/l3/hotels/ttl_strategy_metrics.sql` | FAILING |
| 69 | hotels_impressions_logs_daily_append | `wego_analytics.hotels_searches_details_impressions_logs` | `models/l3/hotels/hotels_searches_details_impressions_logs.sql` | FAILING |
| 70 | hotels_impressions_logs_daily_append_clustered | `wego_analytics.hotels_searches_details_impressions_logs_clustered` | `models/l3/hotels/hotels_searches_details_impressions_logs_clustered.sql` | FAILING |
| 71 | hotels_inventory_matching_analysis_daily_run | `analysis.hotels_inventory_matching_analysis` | `models/l3/hotels/hotels_inventory_matching_analysis.sql` |  |
| 72 | hotels_margins_rules_export_daily_run | `analysis.hotels_margins_rules_export` | `models/l3/hotels/hotels_margins_rules_export.sql` |  |
| 73 | hotels_margins_rules_export_initial_run | `analysis.hotels_margins_rules_export` | `models/l3/hotels/hotels_margins_rules_export.sql` |  |
| 74 | hotels_pricing_daily_append | `analysis.hotels_pricing_analysis` | `models/l3/hotels/hotels_pricing_analysis.sql` |  |
| 75 | hotels_pricing_engine_aggregated_daily | `analysis.hotels_pricing_engine_aggregated` | `models/l3/hotels/hotels_pricing_engine_aggregated.sql` |  |
| 76 | hotels_pricing_engine_daily_append | `analysis.hotels_pricing_engine` | `models/l3/hotels/hotels_pricing_engine.sql` |  |
| 77 | hotels_pricing_engine_ota | `pricing_engine.hotels_pricing_engine_ota` | `models/l3/hotels/hotels_pricing_engine_ota.sql` |  |
| 78 | hotels_provider_pricing_analysis_daily_append | `analysis.hotels_provider_pricing_analysis` | `models/l3/hotels/hotels_provider_pricing_analysis.sql` |  |
| 79 | hotels_rates_analysis | `analysis.hotels_rates_analysis` | `models/l3/hotels/hotels_rates_analysis.sql` | FAILING |
| 80 | meta_hotels_display_price_matching_insert_job | `analysis.meta_hotels_display_price_matching_master` | `models/l3/hotels/meta_hotels_display_price_matching_master.sql` |  |
| 81 | trivago_hotel_bid_list | `analysis.trivago_bid_file_upload` | `models/l3/hotels/trivago_bid_file_upload.sql` |  |
| 82 | Apple Search Ads AF InApp Flattening | `apple_search_ad.af_inapp_events` | `models/l3/other/af_inapp_events.sql` |  |
| 83 | KSA_EG_VAT_Reporting | `aaaaa_temporary_export_folder.ksa_eg_vat_reporting` | `models/l3/other/ksa_eg_vat_reporting.sql` | FAILING |
| 84 | Sync booking.com assumed gross margin at 11:00am everyday | `aaaaa_temporary_export_folder.booking_com_assumed_gross_margin` | `models/l3/other/booking_com_assumed_gross_margin.sql` |  |
| 85 | Wenrix export sample | `aaaaa_temporary_export_folder.wenrix_export_sample_data` | `models/l3/other/wenrix_export_sample_data.sql` |  |
| 86 | corporate_code_performance_analysis | `analysis.corporate_code_performance_analysis` | `models/l3/other/corporate_code_performance_analysis.sql` |  |
| 87 | destination_aggregated_2 | `analysis.destination_aggregated_2` | `models/l3/other/destination_aggregated_2.sql` |  |
| 88 | distribution_user_analysis_daily | `analysis.distribution_user_analysis_daily` | `models/l3/other/distribution_user_analysis_daily.sql` |  |
| 89 | dynamic_pricing_ab_test | `analysis.dynamic_pricing_ab_test` | `models/l3/other/dynamic_pricing_ab_test.sql` |  |
| 90 | dynamic_pricing_markup_analysis | `analysis.dynamic_pricing_markup_markdown_analysis` | `models/l3/other/dynamic_pricing_markup_markdown_analysis.sql` |  |
| 91 | ota_data_upload_template_working_30Oct24 | `ota_data_upload_template_working_30oct24.bu_code` | `models/l3/other/bu_code.sql` |  |
| 92 | persona_level_raw | `analysis.persona_level_raw` | `models/l3/other/persona_level_raw.sql` |  |
| 93 | pricing_engine_analysis | `analysis.pricing_engine_analysis` | `models/l3/other/pricing_engine_analysis.sql` |  |
| 94 | promo_issuance_analysis | `analysis.promo_issuance_analysis` | `models/l3/other/promo_issuance_analysis.sql` |  |
| 95 | provider_code_domain_mapping | `analysis.provider_domain_code_mapping` | `models/l3/other/provider_domain_code_mapping.sql` |  |
| 96 | rfm_summary | `rfm_analysis.summary` | `models/l3/other/summary.sql` | FAILING |
| 97 | searches_clicks_destination_aggregated | `analysis.searches_clicks_destination_aggregated` | `models/l3/other/searches_clicks_destination_aggregated.sql` |  |
| 98 | wego_aggregated_master_append_version | `analysis.wego_aggregated_master` | `models/l3/other/wego_aggregated_master.sql` |  |
| 99 | wego_user_journey_analysis_daily_append | `analysis.wego_user_journey_analysis` | `models/l3/other/wego_user_journey_analysis.sql` |  |
| 100 | white_label_ancillaries_analysis_daily_append | `analysis.white_label_ancillaries_analysis` | `models/l3/other/white_label_ancillaries_analysis.sql` |  |
| 101 | Apple Search Ads Mapping tables flatenning | `apple_search_ad.compiled_campaign` | `models/l3/platform/compiled_campaign.sql` |  |
| 102 | app_installs_master_daily_append_v2 | `analysis.app_installs_master` | `models/l3/platform/app_installs_master.sql` |  |
| 103 | app_installs_master_ios_ssot_daily_run | `analysis.app_installs_master_ios_ssot` | `models/l3/platform/app_installs_master_ios_ssot.sql` |  |
| 104 | bow_flights_supplier_fare_analysis_v3 | `analysis.bow_flights_supplier_fare_analysis_v3` | `models/l3/platform/bow_flights_supplier_fare_analysis_v3.sql` |  |
| 105 | campaign_name_recent_name_mapping | `analysis.campaign_name_recent_name_mapping` | `models/l3/platform/campaign_name_recent_name_mapping.sql` |  |
| 106 | competitor_installs_session_data | `analysis.competitor_installs_session_data` | `models/l3/platform/competitor_installs_session_data.sql` |  |
| 107 | flights_to_hotels_cross_sell_master | `analysis.flights_to_hotels_cross_sell_master` | `models/l3/platform/flights_to_hotels_cross_sell_master.sql` |  |
| 108 | ltv_unique_new_clients_raw | `wego_LTV.ltv_unique_new_clients_raw` | `models/l3/platform/ltv_unique_new_clients_raw.sql` |  |
| 109 | marketing_landing_pages_daily_append | `analysis.marketing_landing_pages` | `models/l3/platform/marketing_landing_pages.sql` |  |
| 110 | netsuite_group_pnl | `analysis.netsuite_group_pnl` | `models/l3/platform/netsuite_group_pnl.sql` |  |
| 111 | new_vs_returning_revenue_bookers_cohortM | `wego_LTV.new_vs_returning_revenue_bookers_cohortm` | `models/l3/platform/new_vs_returning_revenue_bookers_cohortm.sql` |  |
| 112 | new_vs_returning_revenue_cohortM | `wego_LTV.new_vs_returning_revenue_cohortm` | `models/l3/platform/new_vs_returning_revenue_cohortm.sql` |  |
| 113 | new_vs_returning_sessions_bookers_cohortM | `wego_LTV.new_vs_returning_sessions_bookers_cohortm` | `models/l3/platform/new_vs_returning_sessions_bookers_cohortm.sql` |  |
| 114 | new_vs_returning_sessions_cohortM | `wego_LTV.new_vs_returning_sessions_cohortm` | `models/l3/platform/new_vs_returning_sessions_cohortm.sql` |  |
| 115 | new_vs_returning_users_bookers_cohortM | `wego_LTV.new_vs_returning_users_bookers_cohortm` | `models/l3/platform/new_vs_returning_users_bookers_cohortm.sql` |  |
| 116 | new_vs_returning_users_cohortM | `wego_LTV.new_vs_returning_users_cohortm` | `models/l3/platform/new_vs_returning_users_cohortm.sql` |  |
| 117 | platform_install_costs_app_data_final | `analysis.platform_install_costs_app_data_final` | `models/l3/platform/platform_install_costs_app_data_final.sql` |  |
| 118 | rtbhouse_conversion_report | `marketing_analytics.rtbhouse_conversion_report` | `models/l3/platform/rtbhouse_conversion_report.sql` |  |
| 119 | shopcash_clients_master_table | `shopcash_analytics.shopcash_clients_master_table` | `models/l3/platform/shopcash_clients_master_table.sql` |  |
| 120 | shopcash_lifetime_value_ltv_master | `analysis.shopcash_lifetime_value_ltv_master` | `models/l3/platform/shopcash_lifetime_value_ltv_master.sql` |  |
| 121 | shopcash_pageviews_analysis_daily_append | `shopcash_analytics.shopcash_pageviews_analysis` | `models/l3/platform/shopcash_pageviews_analysis.sql` |  |
| 122 | trains_clicks_daily_append | `analysis.trains_clicks` | `models/l3/platform/trains_clicks.sql` |  |
| 123 | trains_sessions_daily_append | `wego_analytics.trains_sessions` | `models/l3/platform/trains_sessions.sql` |  |
| 124 | umrah_user_journey | `analysis.umrah_user_journey` | `models/l3/platform/umrah_user_journey.sql` |  |
| 125 | user_hash_client_id_mapping | `analysis.user_hash_client_id_mapping` | `models/l3/platform/user_hash_client_id_mapping.sql` |  |
| 126 | wego_pageviews_anaysis_price_comparison | `analysis.wego_pageviews_anaysis_price_comparison` | `models/l3/platform/wego_pageviews_anaysis_price_comparison.sql` |  |
| 127 | Skyscanner Bidding ML Model | `skyscanner_bidding.skyscanner_raw_data` | `models/l3/skyscanner/skyscanner_raw_data.sql` |  |
| 128 | bow_hotels_skyscanner_clicks_conversions | `analysis.bow_hotels_skyscanner_clicks_conversions` | `models/l3/skyscanner/bow_hotels_skyscanner_clicks_conversions.sql` |  |
| 129 | skyscanner_bid_file_upload | `wego_analytics.skyscanner_bid_file_upload` | `models/l3/skyscanner/skyscanner_bid_file_upload.sql` |  |

Enabled queries with a parsed destination: 129. Chain-head tables (read by
other queries) land in l2 and migrate FIRST, so their dependents can ref() them
instead of bridging.
