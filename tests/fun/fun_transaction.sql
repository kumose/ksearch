insert into Kstest.student(id, name1, name2, age1, age2, class1, class2, address, height)
values (1, 'ks1', 'baidu1', 1, 1, 1, 1, 'baidu1', 1);
insert into Kstest.student(id, name1, name2, age1, age2, class1, class2, address, height)
values (2, 'ks2', 'baidu2', 2, 2, 2, 2, 'baidu2', 2);
select *
from Kstest.student;
begin;
insert into Kstest.student(id, name1, name2, age1, age2, class1, class2, address, height)
values (3, 'ks3', 'baidu3', 3, 3, 3, 3, 'baidu3', 3);
update student
set id=4,
    name1='a',
    name2='b',
    class1=10,
    class2=11
where class1 = 2;
update student
set id=4,
    name1='a',
    name2='b',
    class1=10,
    class2=11
where class1 = 10;
commit;
select *
from Kstest.student;
begin;
commit;
begin;
begin;
commit;
begin;
rollback;
begin;
rollback;
rollback;
rollback;
rollback;
begin;
commit;
commit;
commit;
commit;
set
autocommit=0;
insert into Kstest.student(id, name1, name2, age1, age2, class1, class2, address, height)
values (5, 'ks5', 'baidu5', 5, 5, 5, 5, 'baidu5', 5);
update Kstest.student
set id=6,
    name1='a',
    name2='b',
    class1=10,
    class2=1
where class1 = 10;
update Kstest.student
set name1='a',
    name2='b',
    class1=3,
    class2=1
where class1 = 10;
commit;
select *
from Kstest.student;
begin;
update Kstest.student
set name1='a',
    name2='b',
    class1=3,
    class2=1
where class1 = 10;
commit;
select *
from Kstest.student;
set
autocommit=1;
update Kstest.student
set name1='a',
    name2='b',
    class1=3,
    class2=1
where class1 = 10;
select *
from Kstest.student;
select *
from Kstest.student
where class1 = 10;
