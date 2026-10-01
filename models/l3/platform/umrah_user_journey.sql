{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : umrah_user_journey
-- Destination: analysis.umrah_user_journey  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table analysis.umrah_user_journey 
-- partition by application_start_date as 

with base as 
(SELECT 
* except(former_event_id,following_page_name,following_page_type,following_page_subtype,last_page_subtype,advertiser_id)
 FROM `wego-cloud.analysis.wego_pageviews_analysis` WHERE TIMESTAMP_TRUNC(created_at, DAY) >= TIMESTAMP("2026-06-01") and TIMESTAMP_TRUNC(created_at, DAY) <= timestamp(current_date())
and product = "umrah"
),


ref as
(with pageviews as 
(select pageview_id,former_event_id FROM `wego-cloud.wego_analytics.pageviews`  WHERE TIMESTAMP_TRUNC(created_at, DAY) >= TIMESTAMP("2026-06-25")  and product= "umrah" group by 1,2)


select a.*, b.ref as ref from pageviews as a left join `wego-cloud.pilgrim_services.pilgrim_packages` as b 
on cast(a.former_event_id as string) = cast(b.id as string)


),

base_ref_pre as  
(
select a.*, b.ref,

if(row_number() over (partition by b.ref, session_id order by created_at) = 1, 1, 0) as new_session_flag


 from base as a left join ref as b on a.pageview_id = b.pageview_id
),


base_ref as   
(
select *, sum(new_session_flag) over(partition by ref order by created_at) as cum_sessions from base_Ref_pre


),


application_start as    
(
select user_hash,ref,max(user_country_code) as user_country_code,
min(case when page_name = "Applicants" then created_at end) as application_start_ts,
from base_Ref group by 1,2
),

time_spent as  
(
select user_hash,ref,session_id,page_name,
timestamp_diff(max(created_at),min(created_at),SECOND) as duration,max(created_at) as last_visited_ts,min(created_at) as first_visited_ts

 from base_ref where page_name in ("Applicants","Timeline","Documents","Review & Payment","Confirmation")  group by 1,2,3,4


),

time_spent_per_page as  
(
select user_hash,ref,page_name,
sum(duration) as duration, min(first_visited_ts) as first_visited_ts,max(last_visited_ts) as last_visited_ts

from time_spent group by 1,2,3
),

application_process as    
(
select user_hash,ref,page_name,
max(case 
when (page_name = "Applicants" and event_category = "form_submission" and event_object = "application_form" and event_action = "submit_validation")
or (event_category = "document_upload"
     and event_action   = "success"
     and event_object in ("personal_photo","passport_bio","vaccination","iqama"))

then created_at end) as submission_ts ,

max(case 
when (page_name = "Applicants" and event_category = "form_submission" and event_object = "application_form" and event_action = "submit_validation")
or (event_category = "document_upload"
     and event_action   = "success"
     and event_object in ("personal_photo","passport_bio","vaccination","iqama"))

then cum_sessions end) as sessions_taken ,
from base_ref where page_name in ("Applicants","Documents")  group by 1,2,3

union all 

select user_hash,ref,page_name  ,
min(created_at) as submission_ts ,
min(cum_Sessions) as sessions_taken
from base_ref where page_name = "Confirmation"  group by 1,2,3

union all 

select user_hash,ref,page_name  ,
max(created_at) as submission_ts ,
max(cum_Sessions) as sessions_taken
from base_ref where page_name = "Review & Payment" and event_category = "review" and event_object = "complete_payment_button" and event_action = "click"


 group by 1,2,3


),


final as (

select 
a.user_hash,
a.ref, 
a.application_start_ts,
b.page_name,
b.duration,
b.first_visited_ts,
b.last_visited_ts,
c.submission_ts,
c.sessions_taken
from application_start as a 
left join time_spent_per_page as b 
on a.user_hash = b.user_hash 
and a.ref = b.ref
left join application_process as c
on b.user_hash = c.user_hash 
and b.ref = c.ref 
and b.page_name = c.page_name

where a.ref is not null



)

select 
a.*,
date(application_start_ts) as application_start_date,
application_start_ts,
page_name,
duration,
first_visited_ts,
last_visited_ts,
submission_ts,
sessions_taken,
c.site_Code,
c.device_type,
c.channel,
c.market,
c.locale,
c.new_vs_returning_status,
c.user_country_code


from  `wego-cloud.pilgrim_services.pilgrim_packages` as a 
left join final as b 
on a.ref = b.ref 
and a.user_hash = b.user_hash

left join (select 
ref,site_Code,device_type,channel,market,locale,new_vs_returning_status,user_country_code from 
base_ref
group by 1,2,3,4,5,6,7,8

  
) as c   
on a.ref = c.ref
where date(application_start_ts) <= current_date() - 1
and a.user_hash 
NOT IN ('bcad15b4b8142370dfee527d22dd5def',
'da1a726d41424c8233477d5197303ecd',
'de03b0680d828dc7d16aee6b5d8a5dcb',
'37e249baaed67436ad6ca5ad92caa551',
'acff582e8ffcc8609f787035ee48a890',
'6e32b3f94a2673412f5f3de77a2098f7',
'faa6a811a8fd11bde3843f7ddebca445',
'2a406ec67c772211b54dc62b3eddf3ba',
'f71c2094f9a9a83acd4cc62de75b0c71',
'9f60229095345c7c4784a8219bb73a16',
'dcf3c9597c4c783d63e906131cdb74e1',
'd9c2655173ed34b4d23a43561e7fdfb7',
'3d6a9b368c4f347de7dd9706f108ad02',
'c04426be85270e4e90cc97133f9ce665',
'1f19e0b17232362850b1b9ab90362cc8',
'aa7c2d57c7156ea3faa32b9d64b245c0',
'28470612ec02ce7ffa7d608e8d6c34ce',
'1b12b3c9b38f97c16b2215bbb39f32cf',
'444c0ab029766bf8f2499f50eb95a207',
'4b852b4f15fe6e92605d8e189fbfbc85',
'9b47a8c6ef2fa5c84ad00c4632cf743d',
'9bcc3d7234a232aa13e1935e3cb248d6',
'62a36845ae970a23fc2539f3526b629a',
'6c0ae6f055369fc40397d57e819ac104',
'6ac9de5abd45db8d0baa3c8fd51a1129',
'e6f089df8af6bfc509c5f6800ff0b2ae',
'bab85c13897928d004f968d88456e257',
'e2c81c8c7722dc2ee71eb2ea18f46952',
'a1de745d9725a25a2504aa20eecc49ff',
'f41a11f803e10e0d96d9d27c99a51c4f',
'94bb976cffb15eb88dab4e9bb7a8cd66',
'3fbe89a021d1d384a4d0ae5149b37ccd',
'17eab639d9fc06d2f395f705edab1072',
'0bb920831012663aac02f0d898f1eba5',
'bd914d914cce75ecea3292978a20ae83',
'7838b3dcc8a69f4b2ae4908f7284f2e4',
'63cf88f3edb08ea7d7be6c36b371deeb',
'affde809ca0f10c048df62c90060e79c',
'0f58cda677bddb716be309f7858163d8',
'a937feff9b810e214ce4c5feb201a303',
'589265d31d32add3bc12f8e8d708af16',
'f2dea1c218bac6e134280000221e1115',
'36cca92c4776bc5792d59d08b8cee8ea',
'b23c63295922d60942ab6a0d82db81eb',
'4c24a1c583f97c07e6dbe9e9c674cd86',
'1ca1c73a25b0fd09d228cd7416d29225',
'e60581617b690d217850d3188003a6b1',
'8c75de670ec362dec7b27207de416725',
'f2cd2f768aff9d0041285c8bd8d706c0',
'3de18042ece9cd57edea819e7b4e22c2',
'4cd89cd94bb14ab19f8ad3854e375e11',
'fc65bd5bda40f2b7d48422ece9aa909e',
'4d8de2e4f62948f1e71afe01bde65465',
'eafb196354fc088a3c16e37dcd25aa15',
'25748ac5747cc659b101fc1fc512a6ea',
'12e1a51ba7c5ae92f1a1f2c83b43d6eb',
'8c531c0fcb36d0f77cb8b7f6404b87f1',
'12d3d6285c6f5ebf02484812fc1876b1',
'9d491833151bd41218721c9e696c708d',
'43442c3fa12cf958cea31892a7c1ed20',
'5b1e0fb0a0a52716971575afd6c50a77',
'e810273d1fc2175c424c94be8acc5314',
'64f5a080f857cf61526dcef75f81a050',
'f00636ed3057a7c10f882ea676002e2a',
'785061e6d191a06e78a3c7005c0b8f5a',
'37fd43609636c8914f821f3202e53738',
'1966569fd6f73ed095c8a99e43ce69af',
'ebacc210134fc163a9e7482b5958e735',
'8492a34e8f3010a0c51f011fc07cf725',
'3f0c14e76a3f4a5ba9111616fc8dffe2',
'e18aae100ecaa00d1c209cc63c227f61',
'286c5e234ff71907f7b7574249aea25b',
'fade4d005815fc72a5afba4615a2201e',
'f74ff458af01ffffd58b09db0900fdeb',
'fc06673805a4241d01d1830af555ef78',
'8dd4a480cbca440508998a78e2ff783a',
'764c1490b1276423022183f814e3236a',
'e26243c3e9f48080b7e05fe40dff8be7',
'914ffaf40ef544855147f9271e7d9247',
'ee10f3db3cd12d9f5072d15f4727a27c',
'909e9d3787e3294aceb5126bddd0e41d',
'aecf729a945a0ad3a8812bc43d524ae2',
'81708b70180819a9995c193c29ccae97',
'95b1ddb5035fccbc7c30e37bd0700925',
'39fe30a96f091ab4ddc88a2ac1181ff5',
'be254e6dddae140b84c006d2f34f90c0',
'08558ec42820ee01c20c90d787bc680b',
'37646bf07e6f7af8c9569eb0c82cc0c7',
'a4ee6230bd339f94875da059e9166378',
'84d9de2dbf61d2a3a5fb46aea8100f97',
'1c078d5c39f75b5fdde0bc8be66fab9b',
'cc4b0404ff217aeb7fbe51a546111d5f',
'5c3bb44e02377bf9c52c7d5cc600ad6c',
'6c6144ab87fc78dc10ed5c241493e271',
'175e236840af2eb769031e948a7c78d8',
'404d62ea59685023cbe0b4be0b3c71fc',
'1cb996ec2902c0b227373f96ae6650f4',
'981cfe6d69ad670ebe852d87a2734eaf',
'abfa7315e8bd73e2b4f5f4deed877323',
'f1fd86959acfc60e39f2cb6df5ebd14a')
{% endraw %}
