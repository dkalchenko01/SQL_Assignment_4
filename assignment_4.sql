-- DATA PREPARATION AND PARSING
create or replace table raw_games as
select
    unnest(games) as games -- metadata is not needed, taking only games
from read_json('/Users/dkalchenko/Downloads/steam_2025_5k-dataset-games_20250831.json', maximum_object_size = 106144477);

create or replace table games as
select
    cast(json_value(games, '$.appid') as int64) as appid,
    trim(both '"'  from json_value(games, '$.app_details.data.type')) as type,
    trim(both '"'  from json_value(games, '$.app_details.data.name')) as name,
    cast(trim(both '"'  from json_value(games, '$.app_details.data.required_age')) as int64) as required_age,
    cast(json_value(games, '$.app_details.data.is_free') as boolean) as is_free,
    cast(json_value(games, '$.app_details.data.platforms.windows') as boolean) as windows,
    cast(json_value(games, '$.app_details.data.platforms.mac') as boolean) as mac,
    cast(json_value(games, '$.app_details.data.platforms.linux') as boolean) as linux,
    cast(json_value(games, '$.app_details.data.release_date.coming_soon') as boolean) as coming_soon,
    trim(both '"'  from json_value(games, '$.app_details.data.release_date.date')) as release_date,
    trim(both '"'  from json_value(games, '$.app_details.data.price_overview.currency')) as currency,
    cast(json_value(games, '$.app_details.data.price_overview.final') as int64) / 100.0 as final_price,
    cast(json_value(category, '$.id') as int64) as category_id,
    trim(both '"'  from json_value(category, '$.description')) as category_descriprion,
    developer,
    cast(trim(both '"'  from json_value(genre, '$.id')) as int64) as genre_id,
    trim(both '"'  from json_value(genre, '$.description')) as genre_descriprion,
    from raw_games
cross join unnest(games.app_details.data.categories) as cat(category)
cross join unnest(games.app_details.data.developers) as dev(developer)
cross join unnest(games.app_details.data.genres) as gen(genre)
;

-- Sample:
select * from games
limit 10;


create or replace table raw_reviews as
select
    unnest(reviews) as reviews
from read_json('/Users/dkalchenko/Downloads/steam_2025_5k-dataset-reviews_20250901.json', maximum_object_size = 53584150)

create or replace table reviews as
select
    cast(json_value(reviews, '$.appid') as int64) as appid,
    cast(json_value(reviews, '$.review_data.query_summary.num_reviews') as int64) as num_reviews,
    cast(json_value(reviews, '$.review_data.query_summary.review_score') as int64) as review_score,
    cast(json_value(reviews, '$.review_data.query_summary.total_positive') as int64) as total_positive,
    cast(json_value(reviews, '$.review_data.query_summary.total_negative') as int64) as total_negative,
    cast(json_value(reviews, '$.review_data.query_summary.total_reviews') as int64) as total_reviews,
    trim(both '"'  from json_value(reviews, '$.review_data.query_summary.review_score_desc')) as review_score_desc,
    cast(trim(both '"'  from json_value(review, '$.recommendationid')) as int64) as review_id,
    trim(both '"'  from json_value(review, '$.language')) as review_language
from raw_reviews
cross join unnest(reviews.review_data.reviews) as rev(review);

-- Sample:
select * from reviews
limit 10;


-- ANALYTICAL INSIGHTS

/* PART 1.
I've analyzed the top 20 games by the number of reviews and their quality by the percentage of positive/negative reviews.
The results show that most popular games have mostly positive reviews, which proves their good quality.
*/
select
    distinct
    g.appid as appid,
    name as game_name,
    total_reviews as reviews_count,
    concat(round(total_positive / total_reviews * 100), '%') as positive_share,
    concat(round(total_negative / total_reviews * 100), '%') as negative_share
from games g
left join (select distinct appid, total_reviews,
                           total_positive,
                           total_negative
                           from reviews) as r
    on g.appid = r.appid
order by total_reviews desc
limit 20;
/*
359550,Tom Clancy's Rainbow Six® Siege X,1213464,84.0%,16.0%
252490,Rust,1043118,88.0%,12.0%
218620,PAYDAY 2,436678,90.0%,10.0%
252950,Rocket League®,429794,88.0%,12.0%
582010,Monster Hunter: World,301611,89.0%,11.0%
107410,Arma 3,219158,91.0%,9.0%
534380,Dying Light 2 Stay Human: Reloaded Edition,150426,79.0%,21.0%
1716740,Starfield,109645,59.0%,41.0%
232090,Killing Floor 2,84879,89.0%,11.0%
287700,METAL GEAR SOLID V: THE PHANTOM PAIN,68732,92.0%,8.0%
1274570,DEVOUR,64667,91.0%,9.0%
35140,Batman: Arkham Asylum Game of the Year Edition,53915,96.0%,4.0%
424840,Little Nightmares,49335,95.0%,5.0%
1533390,Gorilla Tag,40175,93.0%,7.0%
2321470,Deep Rock Galactic: Survivor,36445,87.0%,13.0%
1088850,Marvel's Guardians of the Galaxy,32264,94.0%,6.0%
337000,Deus Ex: Mankind Divided,29713,77.0%,23.0%
674020,World War 3,28947,60.0%,40.0%
257510,The Talos Principle,28411,95.0%,5.0%
434570,Blood and Bacon,26986,96.0%,4.0%
*/

/* PART 2.
I've analyzed average price in usd by genre and the total average price.
Animation & Modeling appears to be gradually more pricey than other genres.
*/
select distinct currency from games
where currency is not null;

create or replace table currencies(
       currency varchar(3),
       usd_rate double
);
insert into currencies values
    ('KRW', 0.00068),
    ('USD', 1),
    ('PHP', 0.017),
    ('MXN', 0.054),
    ('PLN', 0.27),
    ('EUR', 1.16),
    ('BRL', 0.19),
    ('RUB', 0.013),
    ('KWD', 3.26),
    ('AUD', 0.65),
    ('CAD', 0.71);

with converted_currencies as (
    select
    appid,
    genre_id,
    genre_descriprion,
    round(g.final_price * c.usd_rate, 2) as usd_price
from games g
left join currencies c
on g.currency = c.currency
where final_price is not null
)
select
    distinct
    genre_id,
    genre_descriprion,
    round(avg(usd_price) over(partition by genre_descriprion), 2) as avg_price_usd,
    round(avg(usd_price) over(), 2) as overall_avg_price_usd
from converted_currencies
where genre_descriprion not in ('Инди', 'Экшены', 'Симуляторы')
order by avg_price_usd desc
;
/*
51,Animation & Modeling,69.48,9.97
53,Design & Illustration,21.15,9.97
29,Massively Multiplayer,15.98,9.97
59,Web Publishing,14.82,9.97
58,Video Production,13.84,9.97
56,Software Training,13.61,9.97
60,Game Development,12.97,9.97
54,Education,12.69,9.97
3,RPG,12.55,9.97
37,Free To Play,12.28,9.97
57,Utilities,11.89,9.97
9,Racing,11.82,9.97
2,Strategy,11.76,9.97
18,Sports,11.18,9.97
28,Simulation,11.15,9.97
70,Early Access,10.63,9.97
72,Nudity,10.07,9.97
71,Sexual Content,9.82,9.97
1,Action,9.81,9.97
25,Adventure,9.75,9.97
52,Audio Production,9.72,9.97
23,Indie,8.36,9.97
4,Casual,7.12,9.97
55,Photo Editing,6.89,9.97
73,Violent,6.25,9.97
74,Gore,5.28,9.97
1,Aksiyon,4.49,9.97
29,Multijogador Massivo,3.23,9.97
1,Acción,2.91,9.97
25,Aventura,2.05,9.97
1,Ação,1.82,9.97
28,Simulação,1.8,9.97
2,Strateji,1.49,9.97
28,Simülasyon,1.49,9.97
23,Indépendant,0.6,9.97
*/

/* PART 3.
I've analyzed the platform availability for each game type.
The results represent that windows host almost 100% of all game types, mac and linux are definitely not the best options for game lovers.
 */
with distincts as (
    select distinct
        appid,
        type,
        windows,
        mac,
        linux
    from games
),
platform_counts as (select
    type,
    count(distinct appid) as total_games,
    countif(windows) as windows_games,
    countif(mac) as mac_games,
    countif(linux) as linux_games
from distincts
group by type
)
select
    type,
    total_games,
    round(windows_games / total_games * 100, 2) as windows_pct,
    windows_games,
    round(mac_games / total_games * 100, 2) as mac_pct,
    mac_games,
    round(linux_games / total_games * 100, 2) as linux_pct,
    linux_games
from platform_counts
order by total_games desc;
/*
game,4733,99.98,4732,17.37,822,12.78,605
dlc,1779,100,1779,33.61,598,16.92,301
demo,292,100,292,22.6,66,17.47,51
music,37,100,37,83.78,31,86.49,32
video,10,100,10,90,9,90,9
mod,5,100,5,20,1,60,3
advertising,3,100,3,0,0,0,0
series,1,100,1,100,1,100,1
*/


/* PART 4
I've found the most common categories across all games.
The results show that Single-Player games are the most popular ones and family sharing games are not far from the first place:)
*/
select
    category_descriprion,
    count(distinct appid) as games_count
from games
where type = 'game'
group by category_descriprion
order by games_count desc
limit 20;
/*
Single-player,4482
Family Sharing,4056
Steam Achievements,2164
Full controller support,1218
Steam Cloud,1178
Multi-player,920
Partial Controller Support,572
PvP,540
Co-op,535
Online PvP,396
Steam Trading Cards,379
Online Co-op,361
Remote Play Together,335
Steam Leaderboards,326
Shared/Split Screen,326
Custom Volume Controls,229
Tracked Controller Support,216
Playable without Timed Input,212
VR Only,212
Shared/Split Screen PvP,207
 */

/* PART 5
I've analyzed the distribution of free/paid games by genre (their percentages).
The results represent that most games are paid ones and 100% of demos are free (quite obvious).
*/
with distincts as(
    select distinct
        appid,
        type,
        is_free
    from games
)
select
    type,
    sum(if(is_free = true, 1, 0)) as free_games,
    sum(if(is_free = false, 1, 0)) as paid_games,
    concat(round(sum(if(is_free = true, 1, 0)) / count(distinct appid) * 100, 2), '%') as free_games_pct,
    concat(round(sum(if(is_free = false, 1, 0)) / count(distinct appid) * 100, 2), '%') as paid_games_pct
from distincts
group by type
order by free_games desc;
/*
game,645,4088,13.63%,86.37%
demo,292,0,100.0%,0.0%
dlc,154,1625,8.66%,91.34%
mod,5,0,100.0%,0.0%
music,4,33,10.81%,89.19%
series,1,0,100.0%,0.0%
video,0,10,0.0%,100.0%
advertising,0,3,0.0%,100.0%
 */

-- Saving csv for visualisation

COPY (
with converted_currencies as (
    select
    appid,
    genre_id,
    genre_descriprion,
    round(g.final_price * c.usd_rate, 2) as usd_price
from games g
left join currencies c
on g.currency = c.currency
where final_price is not null
)
select
    distinct
    genre_id,
    genre_descriprion,
    round(avg(usd_price) over(partition by genre_descriprion), 2) as avg_price_usd,
    round(avg(usd_price) over(), 2) as overall_avg_price_usd
from converted_currencies
where genre_descriprion not in ('Инди', 'Экшены', 'Симуляторы')
order by avg_price_usd desc
)
TO 'genres.csv' (HEADER, DELIMITER ',');
