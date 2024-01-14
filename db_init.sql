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
create table apt_details(
    id int primary key auto_increment,
    date timestamp,
    city varchar(255),
    district varchar(255),
    voivodeship varchar(255),
    localization_y float,
    localization_x float,
    market varchar(255),
    offer_type varchar(255),
    area float,
    rooms int,
    floor int,
    floors int,
    build_yr int,
    price float,
    url varchar(255),
    price_of_sqm float
);

--@block
create table apt_log(
    id int primary key auto_increment,
    date timestamp,
    auction_links int,
    data_raw int,
    data_clean int,
    ann_r2 float,
    xgb_r2 float,
    prod tinyint
);
