# openGauss-tools-chameleon

#### Overview
Chameleon is a Python 3-based real-time replication tool designed for migrating data from MySQL to openGauss. It leverages the `mysql-replication` library to extract row images from MySQL, which are then stored in openGauss in JSONB format. A `PL/pgSQL` function within openGauss decodes this JSONB data to replay the changes. Additionally, the tool performs an initial full data load in read-only mode, enabling both full data replication and subsequent real-time incremental replication.

The initial codebase of Chameleon is derived from [pg_chameleon](https://github.com/the4thdoctor/pg_chameleon).

#### Features
⦁	It provides online real-time replication by reading the MySQL binary logs (binlogs).

⦁	It reads data from multiple MySQL schemas and restores it to the target openGauss database, supporting different names for source and target schemas.

⦁	It implements real-time replication via a daemon consisting of two subprocesses: one for reading MySQL logs and the other for replaying changes to openGauss.

#### Installation
1. Environments: Linux, FreeBSD, and OpenBSD. Installation requires Python (CPython 3.5+), Java, Maven, and Git.
2. Source database: MySQL 5.5+; target database: openGauss 2.1.0+
3. Installation command (recommended in a Python virtual environment): `python setup.py install`

#### Instruction
⦁	Create a Python virtual environment (for example, `python3 -m venv venv`).

⦁	Activate the Python virtual environment (for example, `source venv/bin/activate`).

⦁	Upgrade Pip: `pip install pip --upgrade`.

⦁	Install Chameleon: `python setup.py install`.

⦁	Create a replication user (for example, `usr_replica`) in MySQL.

⦁	Grant the user the permission to access the replicated database (for example, `GRANT ALL ON sakila.* TO 'usr_replica'`).

⦁	Grant the user the `RELOAD` permission (for example, `GRANT RELOAD ON \*.\* to 'usr_replica'`).

⦁	Grant the user the `REPLICATION CLIENT` permission (for example, `GRANT REPLICATION CLIENT ON \*.\* to 'usr_replica'`).

⦁	Grant the user the `REPLICATION SLAVE` permission (for example, `GRANT REPLICATION SLAVE ON \*.\* to 'usr_replica'`).

#### Contributions

1. Fork this repository.
2. Create a Feat_xxx branch.
3. Commit the code.
4. Create a pull request.
