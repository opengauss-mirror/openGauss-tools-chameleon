# openGauss-tools-chameleon

#### 介绍
chameleon是一个用Python 3编写的MySQL到openGauss的实时复制工具。工具使用mysql-replication库从MySQL中提取row images，这些row images将以jsonb格式被存储到openGauss中。在openGauss中会执行一个pl/pgsql函数，解码jsonb并将更改重演到openGauss。同时，工具通过一次初始化配置，使用只读模式，将MySQL的全量数据拉取到openGauss，使得该工具提供了初始全量数据的复制以及后续增量数据的实时在线复制功能。

chameleon初始代码源自pg_chameleon (https://github.com/the4thdoctor/pg_chameleon).

#### 特色
⦁	通过读取MySQL的binlog，提供实时在线复制的功能。

⦁	支持从多个MySQL schema读取数据，并将其恢复到目标openGauss数据库中。源schema和目标schema可以使用不同的名称。

⦁	通过守护进程实现实时复制，包含两个子进程，一个负责读取MySQL侧的日志，一个负责在openGauss侧重演变更。

#### 安装教程
1.  安装环境： Linux, FreeBSD, OpenBSD. Python: CPython 3.5+ 
2.  源数据库要求：MySQL 5.5+。目的数据库要求：openGauss 2.0.1+
3.  安装命令（建议在python虚拟环境中安装）：python setup.py install

#### 使用说明
⦁	创建python虚拟环境 (例如 python3 -m venv venv)

⦁	激活python虚拟环境 (例如 source venv/bin/activate)

⦁	升级pip **pip install pip --upgrade**

⦁	安装chameleon **python setup.py install**

⦁	在MySQL侧创建用于复制的用户 (例如 usr_replica)

⦁	给用户赋予被复制数据库的访问权限(例如 GRANT ALL ON sakila.* TO 'usr_replica';)

⦁	给用户赋予RELOAD权限(例如 GRANT RELOAD ON \*.\* to 'usr_replica';)

⦁	给用户赋予REPLICATION CLIENT权限(例如 GRANT REPLICATION CLIENT ON \*.\* to 'usr_replica';)

⦁	给用户赋予REPLICATION SLAVE权限(例如 GRANT REPLICATION SLAVE ON \*.\* to 'usr_replica';)

#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request
