--@block
show tables;

--@block
create table apt_links(
    id int primary key auto_increment,
    date timestamp,
    link varchar(255)
);

--@block
create table apt_details_raw(
    id int primary key auto_increment,
    date timestamp,
    city varchar(255),
    district varchar(255),
    voivodeship varchar(255),
    localization_y varchar(255),
    localization_x varchar(255),
    market varchar(255),
    offer_type varchar(255),
    area varchar(255),
    rooms varchar(255),
    floor varchar(255),
    floors varchar(255),
    build_yr varchar(255),
    price varchar(255),
    url varchar(255)
);

--@block
select * from apt_links;

--@block
select date, count(1)
from apt_links 
group by date
order by date desc;

--@block
select * from apt_details_raw;

--@block
select date, count(1)
from apt_details_raw
group by date
order by date desc;
