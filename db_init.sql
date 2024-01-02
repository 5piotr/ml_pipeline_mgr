--@block
create table apt_links(
    id int primary key auto_increment,
    date timestamp,
    link varchar(255)
);

--@block
insert into apt_links (date, link)
values
('2022-01-01','asdf'),
('2023-01-05 20:21:15','śżp');

--@block
select * from apt_links;

--@block
select date, count(1)
from apt_links 
group by date
order by date desc;
