drop table users;
create table users (
    id number generated always as identity,
    username varchar2(255) not null unique,
    email varchar2(255) not null unique,
    hashed_password varchar2(255) not null,
    is_active number default 1,
    primary key (id)
);

insert into users (username, email, hashed_password) values ('testuser', 'testuser@example.com', 'hashedpassword');
insert into users (username, email, hashed_password) values ('brudow', 'brudow@example.com', 'password');

insert into users (username, email, hashed_password) values ('admin', 'admin@example.com', '');

commit;