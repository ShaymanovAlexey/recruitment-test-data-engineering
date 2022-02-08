drop table if exists examples;
drop table if exists places;

create table `examples` (
  `id` int not null auto_increment,
  `name` varchar(80) default null,
  primary key (`id`)
);

create table `places` (
   `id` int not null auto_increment,
   `city` varchar(80) default null,
   `county` varchar(80) default null,
   `country` varchar(80) default null,
   primary key (`id`)
);	

