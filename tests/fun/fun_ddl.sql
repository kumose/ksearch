#
添加列
alter table Kstest.DDLtable
    add column status int(4) default 0 comment "状态：0-上线；1-下线";
sleep
10
desc Kstest.DDLtable;

#
修改列名
alter table Kstest.DDLtable RENAME COLUMN status TO status2;
sleep
10
desc Kstest.DDLtable;

#
删除列
alter table Kstest.DDLtable drop column status2;
sleep
10
desc Kstest.DDLtable;


#
添加索引
alter table Kstest.DDLtable
    add unique index index_name1(name1);
sleep
10
desc  Kstest.DDLtable;

#
删除索引
alter table kstest.DDLtable2 drop index class1_key;
sleep
10
desc  Kstest.DDLtable2;


#
修改表名
alter table Kstest.DDLtable rename to Kstest.DDLtable22;
sleep
10
use Kstest;
show
tables;
#
表名再改回来
alter table Kstest.DDLtable22 rename to Kstest.DDLtable;
sleep
10
use Kstest;
show
tables;

#
drop table
    drop table Kstest.DDLtable;
sleep
10
use Kstest;
show
tables;

#
restore table
restore table Kstest.DDLtable;
sleep
10
use Kstest;
show
tables;

alter table Kstest.DDLtable
    add unique index global global_index_name2(name2);
alter table Kstest.DDLtable drop index global_index_name2;
alter table Kstest.DDLtable
    add index global global_index_class1(class1);
alter table Kstest.DDLtable drop index global_index_class1;
sleep
10
desc  Kstest.DDLtable;
use
Kstest;
show
tables;

#
权限异常分支验证
