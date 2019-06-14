create table conversation
(
    id         serial primary key,
    title      varchar(255) default null,
    creator_id int          default null,
    created_at timestamp    default null,
    updated_at timestamp    default null
);

create table message
(
    id              serial primary key,
    conversation_id int not null,
    sender_id       int not null,
    message         varchar   DEFAULT NULL,
    created_at      timestamp DEFAULT NULL
);

create table participant
(
    id              serial primary key,
    conversation_id int not null,
    users_id        int not null,
    created_at      timestamp DEFAULT NULL,
    updated_at      timestamp DEFAULT NULL
);

create table contact
(
    id         serial primary key,
    user_id    int not null,
    fullname   varchar(255) default null,
    created_at timestamp    default null,
    updated_at timestamp    default null,
    status     bit          default '1'
);

create table user_contact
(
    id         serial primary key,
    user_id    int not null,
    contact_id int not null,
    fullname   varchar(255) default null,
    created_at timestamp    default null,
    updated_at timestamp    default null,
    status     bit          default '1'
);

create table device
(
    id         serial primary key,
    user_id    int not null,
    type       varchar(255) default null,
    token      varchar(255) default null,
    created_at timestamp    default null,
    updated_at timestamp    default null
);

create table handyman
(
    id         serial primary key,
    user_id    int not null,
    fullname   varchar(255) default null,
    updated_at timestamp    default null
);

create table customer
(
    id         serial primary key,
    user_id    int not null,
    fullname   varchar(255) default null,
    updated_at timestamp    default null
);

create table users
(
    id          serial primary key,
    email       varchar(255) default null,
    password    varchar(255) default null,
    is_active   bit          default '1',
    is_reported bit          default '0',
    is_block    bit          default '0',
    created_at  timestamp    default null,
    updated_at  timestamp    default null
);


-- #################################################################################################################
-- #                                            CREATE PROCEDURE                                                   #
-- #################################################################################################################
create or replace function sp_InsertContact(user_handyman int, user_customer int) -- id_user: handyman && contact_user: customer
    returns boolean as
$$
declare
    _isCheck    boolean default false;
    _fullName1  varchar(255) default null;
    _fullName2  varchar(255) default null;
    _contact_id int default 0;

begin
    if exists(select *from handyman where user_id = user_handyman) then
        -- insert contact
        select fullname into _fullName1 from customer where user_id = user_customer;
        insert into contact(user_id, fullname, created_at)
        values (user_customer, _fullName1, now());

        -- insert user_contact
        select id into _contact_id from contact order by id desc limit 1;
        select distinct h.fullname into _fullName2
            from handyman as h
        where h.user_id = user_handyman;

        insert into user_contact(user_id, contact_id, fullname, created_at)
        values (user_handyman, _contact_id, _fullName2, now());

        select true into _isCheck;
    else
        select false into _isCheck;
    end if;
    return _isCheck;
end;
$$
    language plpgsql;

-- #################################################################################################################

create or replace function sp_CheckUserContact(user_handyman_id int, user_customer_id int)
    returns boolean as
$$
declare
    _check      boolean default false;
    _contact_id int default 0;
begin
    if exists(select *from contact as c join user_contact as u on c.id = u.contact_id
                where c.user_id = user_customer_id and u.user_id = user_handyman_id) then
        select c1.id into _contact_id from contact as c1 join user_contact as u1 on c1.id = u1.contact_id
        where c1.user_id = user_customer_id and u1.user_id = user_handyman_id;

        update contact set status = '1' where id = _contact_id;

        select true into _check;
    else
        select false into _check;
    end if;
    return _check;
end;
$$ language plpgsql;


-- #################################################################################################################
-- drop function sp_GetListContact(integer);
create or replace function sp_GetListContact(uid int)
    returns table
            (
                c_id         int,
                c_user_id    int,
                c_fullname   varchar(255),
                c_created_at timestamp
            )
as
$$
begin
    if exists(select * from handyman where user_id = uid) then
       return query select c.id, c.user_id, c.fullname, c.created_at
        from user_contact as u
                 join contact as c on c.id = u.contact_id
        where u.user_id = uid and c.status = '1';

    elseif exists(select * from customer where user_id = uid) then
        return query select u.contact_id, u.user_id, u.fullname, u.created_at
        from user_contact as u
                 join contact as c on c.id = u.contact_id
        where c.user_id = uid and u.status = '1';
    end if;
end;
$$ language plpgsql;

-- select *from sp_GetListContact(1);


-- #################################################################################################################

create or replace function sp_DeleteContact(id_contact int, id_user int)
    returns boolean as
$$
declare
    _isCheck boolean default false;
begin
    if exists(select *from contact c
                       join user_contact u on u.contact_id = c.id
              where u.contact_id = id_contact) then
        if exists(select *from handyman where user_id = id_user) then
            update contact set status = '0' where id = id_contact;
            select true into _isCheck;
        elseif exists(select *from customer where user_id = id_user) then
            update user_contact set status = '0' where id = id_contact;
            select true into _isCheck;
        else
            select false into _isCheck;
        end if;
    end if;
    return _isCheck;
end;
$$ language plpgsql;

-- select sp_DeleteContact(3,1);
-- #################################################################################################################

create or replace function sp_SaveMessage(_sender_id int, _participant_id int, _message_reply varchar(255))
    returns boolean as
$$
    --
declare
    _conversation_id int default 0;
    _title           varchar(255) default null;
    _isCheck         boolean default false;
    _str             varchar(255) default null;
    _deviceToken     varchar(255) default null;

    --
begin
    if exists(select *from handyman where user_id = _participant_id) then
        select fullname into _title from handyman where user_id = _participant_id;
        select d.token
        into _deviceToken
        from device as d
                 join users as u on d.user_id = u.id
                 join handyman h on u.id = h.user_id
        where h.user_id = 1;
    end if;

    --
    if exists(select *from customer where user_id = _participant_id) then
        select fullname into _title from customer where user_id = _participant_id;
        select d.token
        into _deviceToken
        from device as d
                 join users as u on d.user_id = u.id
                 join customer c on u.id = c.user_id
        where c.user_id = _participant_id;
    end if;

    --
    if exists(select *from conversation where creator_id = _sender_id) then
        if exists(select *from participant as p
                           join conversation c on c.id = p.conversation_id
                  where c.creator_id = _sender_id and p.users_id = _participant_id) then
            select p.conversation_id into _conversation_id
            from participant as p
                join conversation as c on p.conversation_id = c.id
            where c.creator_id = _sender_id and p.users_id = _participant_id;

            insert into message(conversation_id, sender_id, message, created_at)
            values (_conversation_id, _sender_id, _message_reply, now());

--                 set _str = 'convesation-true';
            select true into _isCheck;
        else
            select false into _isCheck;
--                 set _str = 'conversation-false';
        end if;
    end if;

    if _isCheck = false then
        if exists(select *from participant p where p.users_id = _sender_id) then
            if exists(select *
                      from participant as p
                               join conversation as c on c.id = p.conversation_id
                      where p.users_id = _sender_id
                        and c.creator_id = _participant_id) then
                select p.conversation_id
                into _conversation_id
                from participant as p
                         join conversation as c on p.conversation_id = c.id
                where p.users_id = _sender_id
                  and c.creator_id = _participant_id;

                insert into message(conversation_id, sender_id, message, created_at)
                values (_conversation_id, _sender_id, _message_reply, now());

                select true into _isCheck;

--                 set _str = 'participant-true';
            else
                select false into _isCheck;

--                 set _str = 'participant-false';
            end if;
        else

            --
            insert into conversation(title, creator_id, created_at)
            values (_title, _sender_id, now());
            --
            select id
            into _conversation_id
            from conversation
            order by id desc
            limit 1;
            --
            insert into message(conversation_id, sender_id, message, created_at)
            values (_conversation_id, _sender_id, _message_reply, now());
            --
            insert into participant(conversation_id, users_id, created_at)
            values (_conversation_id, _participant_id, now());

            select true into _isCheck;

        end if;
    end if;

--
    return _isCheck;
end;

$$ language plpgsql;

-- select sp_SaveMessage(8,1,'Xin chao 8-1-1');
--
-- select *from conversation;
-- select *from message;
-- select *from participant;

-- #################################################################################################################

create or replace function sp_GetDeviceTokenByUserId(userId int)
    returns varchar(255) as
$$
declare
    _deviceToken varchar(255) default null;
begin
    if exists(select *from handyman where user_id = userId) then
        select d.token
        into _deviceToken
        from device d
                 join users u on d.user_id = u.id
                 join handyman h on u.id = h.user_id
        where h.user_id = userId;

    elseif exists(select *from customer where user_id = userId) then
        select d.token
        into _deviceToken
        from device d
                 join users u on d.user_id = u.id
                 join customer c on u.id = c.user_id
        where c.user_id = userId;
    else
        select 'error' into _deviceToken;
    end if;
    return _deviceToken;
end;
$$ language plpgsql;

-- select sp_GetDeviceTokenByUserId(1);
-- select *from device;

-- #################################################################################################################

create or replace function sp_ShowMessageAndConversation(id_user int)
    returns table
            (
                m_conversation int,
                m_message text
            )
as
$$
begin
    -- group_concat(Mysql) <=> string_agg(Postgres)
    if exists(select *from handyman where user_id = id_user) then
        return query select m1.conversation_id,
           string_agg(m1.message || ',' || m1.id || ',' || p1.users_id || ',' || m1.conversation_id || ',' || m1.created_at,',' order by m1.created_at desc ) mesages
                     from message as m1
                        join conversation as c1 on m1.conversation_id = c1.id
                        join participant as p1 on p1.conversation_id = c1.id
                     where m1.conversation_id = c1.id and (p1.users_id = id_user or c1.creator_id = id_user)
                     group by m1.conversation_id;
    elseif exists(select *from customer where user_id = id_user) then
        return query select m1.conversation_id,
           string_agg(m1.message || ',' || m1.id || ',' || p1.users_id || ',' || m1.conversation_id || ',' || m1.created_at,',' order by m1.created_at desc ) mesages
                     from message as m1
                        join conversation as c1 on m1.conversation_id = c1.id
                        join participant as p1 on p1.conversation_id = c1.id
                     where m1.conversation_id = c1.id
                       and (p1.users_id = id_user or c1.creator_id = id_user)
                     group by m1.conversation_id;
    end if;

end;
$$ language plpgsql;

-- select *from sp_ShowMessageAndConversation(8);

-- #################################################################################################################
create or replace function sp_ShowMessages(_user_id int, _conversation_id int)
    returns table
            (
                m_converation int,
                m_message        text
            )
as
$$
begin
    return query select m1.conversation_id,
                        string_agg(m1.message || ',' || m1.id || ',' || m1.sender_id || ',' || m1.conversation_id || ',' || m1.created_at, ',' order by m1.created_at desc) mesages
                 from message as m1
                          join conversation as c1 on m1.conversation_id = c1.id
                          join participant as p1 on p1.conversation_id = c1.id
                 where m1.conversation_id = c1.id
                   and (p1.users_id = _user_id or c1.creator_id = _user_id)
                   and m1.conversation_id = _conversation_id
                 group by m1.conversation_id;
end;
$$ language plpgsql;


-- select *from sp_ShowMessages(1, 5);

-- #################################################################################################################

create or replace function sp_GetUserNameByUserId(id_user int)
    returns varchar(255)
as
$$
declare
    _fullname varchar(255) default null;
begin
    if exists(select *from handyman where user_id = id_user) then
        select fullname into _fullname from handyman where user_id = id_user;
    elseif exists(select *from customer where user_id = id_user) then
        select fullname into _fullname from customer where user_id = id_user;
    end if;
    return _fullname;
end;
$$ language plpgsql;


-- select *from sp_GetUserNameByUserId(1);

-- #################################################################################################################

create or replace function sp_DeleteMessageByConversation(cid int)
    returns boolean
as
$$
declare
    isCheck boolean default false;
begin
    delete from message where conversation_id = cid;
    select true into isCheck;
    return isCheck;
end;
$$ language plpgsql;

-- select sp_DeleteMessageByConversation(4);

-- #################################################################################################################
create or replace function tinhTong()
    returns integer as
$total$
declare
    c    int default 0;
    d    int default 0;
    tong int default 0;
begin
    c := 1; d := 2; tong := c + d;
    return tong;
end;
$total$ language plpgsql;


select tinhTong() as a;

-- #################################################################################################################

-- call sp_CheckUserContact(1,12);
--
-- select *from contact;
-- select *from user_contact;

-- call sp_ShowMessages(1, 3);

-- delete from message where conversation_id = 2;



-- call sp_SaveMessage(8, 4,'Xin chao 8-4-5');
-- select *from conversation;
-- select *from participant;
-- select *from message;

-- truncate conversation;
-- truncate participant;
-- truncate message;

-- #################################################################################################################

create or replace function sp_Insert(user_handyman int, user_customer int) -- id_user: handyman && contact_user: customer
    returns varchar(255) as
$$
declare
    _isCheck    boolean default false;
    _fullName1  varchar(255) default null;
    _fullName2  varchar(255) default null;
    _contact_id int default 0;

begin
    if exists(select *from handyman where user_id = user_handyman) then
        -- insert contact
        select fullname into _fullName1 from customer where user_id = user_customer;

        select id into _contact_id from contact order by id desc limit 1;
        select distinct h.fullname
--         into _fullName2
        from contact as uc
                 join users as u on u.id = uc.user_id
                 join handyman as h on u.id = h.user_id
        where h.user_id = 4;
        select true into _isCheck;
    else
        select false into _isCheck;
    end if;
    return _fullName2;
end;
$$
    language plpgsql;

-- select sp_Insert(4,10);

-- #################################################################################################################

-- select *from sp_GetListContact(8);
--
-- select *from sp_DeleteContact(44,7);
--
-- select *from sp_CheckUserContact(7,23);
--

select sp_insertcontact(7,23);
select *from contact;
select *from user_contact;

select *from contact;
select *from user_contact;

select

select *from sp_SaveMessage(9,3,'Xin chao 9-3-3');
select *from conversation;
select *from message;
select *from participant;

select *from sp_ShowMessageAndConversation(1);

-- string_agg(m1.message::character varying, ',', m1.id::character varying, ',', p1.users_id::character varying, ',', m1.conversation_id::character varying,',', m1.created_at,',' order by m1.created_at desc) mesages

select m1.conversation_id,
           string_agg(m1.message || ',' || m1.id || ',' || p1.users_id || ',' || m1.conversation_id || ',' || m1.created_at,',' order by m1.created_at desc ) mesages
from message as m1
         join conversation as c1 on m1.conversation_id = c1.id
         join participant as p1 on p1.conversation_id = c1.id
where m1.conversation_id = c1.id and (p1.users_id = 1 or c1.creator_id = 1)
group by m1.conversation_id;

select m.conversation_id, string_agg(m.message::character varying, ',' order by m.conversation_id)
from message m
group by m.conversation_id;

-- truncate conversation;
-- truncate message;
-- truncate participant;

select *from device;

select *from users;
select *from handyman;
select *from customer;

INSERT INTO users(email, password, is_active, is_reported, is_block, created_at, updated_at)
VALUES ('hajphan97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('maiphan97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('tuannguyen97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('dungnguyen97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('phuongphan97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('huenguyen97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('anhphan97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('quannguyen97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('thangnguyen97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('ducpham97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('dungpham97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('kienle97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('thuyvu97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('thaonguyen97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('duynguyen97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('tuancao97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('anhnguyen97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('lamnguyen97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('noidang97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('huytung97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('anhle97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('phucnguyen97@gmail.com','123', '1', '0', '0', NULL, NULL),
       ('sonchu97@gmail.com','123', '1', '0', '0', NULL, NULL);

-- select *from handyman;

INSERT INTO handyman(user_id, fullname, updated_at)
VALUES (1, 'Phan Van Hai', NULL),
       (2, 'Phan Thi Mai', NULL),
       (3, 'Nguyen Van Tuan', NULL),
       (4, 'Nguyen Van Dung', NULL),
       (5, 'Phan Thi Phuong', NULL),
       (6, 'Nguyen Thi Hue', NULL),
       (7, 'Phan Thi Anh', NULL);

-- select *from customer;

INSERT INTO customer(user_id, fullname, updated_at)
VALUES (8, 'Nguyen Van Quan', NULL),
       (9, 'Nguyen Van Thang', NULL),
       (10, 'Pham Minh Duc', NULL),
       (11, 'Pham Quang Dung', NULL),
       (12, 'Le Trung Kien', NULL),
       (13, 'Vu Van Thuy', NULL),
       (14, 'Nguyen Thi Thao', NULL),
       (15, 'Nguyen Ngoc Duy', NULL),
       (16, 'Cao Xuan Tuan', NULL),
       (17, 'Nguyen Lam Anh', NULL),
       (18, 'Nguyen Hung Lam', NULL),
       (19, 'Dang Huu Noi', NULL),
       (20, 'Pham Huy Tung', NULL),
       (21, 'Le Tuan Anh', NULL),
       (22, 'Nguyen Minh Phuc', NULL),
       (23, 'Chu Van Son', NULL);





-- #################################################################################################################
















