import base64
import logging
import os
import re
import subprocess
from enum import Enum, unique


DEFAULT_NUMERIC_PRECISION = 10
DEFAULT_NUMERIC_SCALE = 0

class ColumnType(Enum):
    """
        Some speical column type, pay attention to the enum prefix
    """
    # mysql data type
    M_HEX_BLOB = 'blob'
    M_HEX_T_BLOB = 'tinyblob'
    M_HEX_M_BLOB = 'mediumblob'
    M_HEX_L_BLOB = 'longblob'
    M_S_GIS_MUL_POINT = 'multipoint'
    M_S_GIS_MUL_LINESTR = 'multilinestring'
    M_S_GIS_MUL_POLYGON = 'multipolygon'
    M_S_GIS_GEOCOL = 'geometrycollection'
    M_S_GIS_GEOCOL2 = 'geomcollection'
    M_C_GIS_POINT = 'point'
    M_C_GIS_GEO = 'geometry'
    M_C_GIS_LINESTR = 'linestring'
    M_C_GIS_POLYGON = 'polygon'
    M_JSON = 'json'
    M_BINARY = 'binary'
    M_VARBINARY = 'varbinary'
    M_BIT = 'bit'
    M_DATATIME = 'datetime'
    M_TIMESTAMP = 'timestamp'
    M_DATE = 'date'
    M_INTEGER = 'integer'
    M_MINT = 'mediumint'
    M_TINT = 'tinyint'
    M_SINT = 'smallint'
    M_INT = 'int'
    M_BINT = 'bigint'
    M_VARCHAR = 'varchar'
    M_CHAR_VAR = 'character varying'
    M_TEXT = 'text'
    M_CHAR = 'char'
    M_TIME = 'time'
    M_TTEXT = 'tinytext'
    M_MTEXT = 'mediumtext'
    M_LTEXT = 'longtext'
    M_DECIMAL = 'decimal'
    M_DEC = 'dec'
    M_NUM = 'numeric'
    M_DOUBLE = 'double'
    M_DOUBLE_P = 'double precision'
    M_FLOAT = 'float'
    M_FLOAT4 = 'float4'
    M_FLOAT8 = 'float8'
    M_REAL = 'real'
    M_FIXED = 'fixed'
    M_YEAR = 'year'
    M_ENUM = 'enum'
    M_SET = 'set'
    M_BOOL = 'bool'
    M_BOOLEAN = 'boolean'
    #opengauss data type
    O_INTEGER = 'integer'
    O_BINT = 'bigint'
    O_TIMESTAP = 'timestamp'
    O_TIMESTAP_NO_TZ = 'timestamp without time zone'
    O_DATE = 'date'
    O_TIME = 'time'
    O_TIME_NO_TZ = 'time without time zone'
    O_BLOB = 'blob'
    O_BYTEA = 'bytea'
    O_BIT = 'bit'
    O_NUM = 'numeric'
    O_NUMBER = 'number'
    O_FLOAT = 'float'
    O_BIGSERIAL = 'bigserial'
    O_SERIAL = 'serial'
    O_DOUBLE_P = 'double precision'
    O_DEC = 'decimal'
    O_ENUM = 'enum'
    O_JSON = 'json'
    O_BOOLEAN = 'boolean'
    O_POINT = 'point'
    O_PATH = 'path'
    O_POLYGON = 'polygon'
    O_GEO = 'geometry'
    O_C_BPCHAR = 'bpchar'
    O_C_NCHAR = 'nchar'
    O_C_VARCHAR = 'varchar'
    O_C_VARCHAR2 = 'varchar2'
    O_C_NVCHAR2 = 'nvarchar2'
    O_C_CLOB = 'clob'
    O_C_CHAR = 'char'
    O_C_CHARACTER = 'character'
    O_C_CHAR_VAR = 'character varying'
    O_C_TEXT = 'text'
    O_SET = 'set'

    def __name_start_with(s):
        results = set()
        for k, v in ColumnType.__members__.items():
            if k.startswith(s):
                results.add(v.value)
        return results

    def get_mysql_hexify_always_type():
        return ColumnType.__name_start_with('M_HEX_')

    def get_mysql_postgis_spatial_type():
        return ColumnType.__name_start_with('M_S_GIS_')

    def get_mysql_common_spatial_type():
        return ColumnType.__name_start_with('M_C_GIS_')

    def get_opengauss_char_type():
        return ColumnType.__name_start_with('O_C_')

    def get_opengauss_date_type():
        return {ColumnType.O_TIMESTAP.value, ColumnType.O_TIMESTAP_NO_TZ.value, ColumnType.O_DATE.value,
                ColumnType.O_TIME.value, ColumnType.O_TIME_NO_TZ.value}

    def get_opengauss_hash_part_key_type():
        return {ColumnType.O_INTEGER.value, ColumnType.O_BINT.value, ColumnType.O_C_CHAR_VAR.value, ColumnType.O_C_TEXT.value,
            ColumnType.O_C_CHAR.value, ColumnType.O_NUM.value, ColumnType.O_NUMBER.value, ColumnType.O_DATE.value, ColumnType.O_TIME_NO_TZ.value,
            ColumnType.O_TIMESTAP_NO_TZ.value, ColumnType.O_TIME.value, ColumnType.O_TIMESTAP.value, ColumnType.O_C_BPCHAR.value,
            ColumnType.O_C_NCHAR.value, ColumnType.O_DEC.value}

class sql_token(object):
    """
    The class tokenises the sql statements captured by mysql_engine.
    Several regular expressions analyse and build the elements of the token.
    The DDL support is purposely limited to the following.

    DROP PRIMARY KEY
    CREATE (UNIQUE) INDEX/KEY
    CREATE TABLE
    ALTER TABLE

    The regular expression m_fkeys is used to remove any foreign key definition from the sql statement
    as we don't enforce any foreign key on the PostgreSQL replication.
    """
    def __init__(self):
        """
            Class constructor the regular expressions are compiled and the token lists are initialised.
        """
        self.tokenised = []
        self.query_list = []
        self.pkey_cols = []
        self.ukey_cols = []

        #re for rename items
        self.m_rename_items = re.compile(r'(?:.*?\.)?(.*)\s*TO\s*(?:.*?\.)?(.*)(?:;)?', re.IGNORECASE)
        #re for column definitions
        self.m_columns=re.compile(r'\((.*)\)', re.IGNORECASE)
        self.m_inner=re.compile(r'\((.*)\)', re.IGNORECASE)
        self.m_comment=re.compile(r"""comment\s*"?'?([^"']*)'?"?""", re.IGNORECASE)

        #re for keys and indices
        self.m_idx = re.compile(r',\s*(?:KEY|INDEX)\s*`?(\w*)?`?\s*\((.*?)\)\s*', re.IGNORECASE)
        self.m_idx_2 = re.compile(r',\s*(?:FULLTEXT|SPATIAL)\s*(?:INDEX|KEY)\s*`?(\w*)`?\s*\((.*?)\)\s*', re.IGNORECASE)
        self.m_pkeys=re.compile(r',\s*(?:CONSTRAINT\s*`?(\w*)?`?)?\s*PRIMARY\s*KEY\s*\((.*?)\)\s?', re.IGNORECASE)
        self.m_ukeys=re.compile(r',\s*(?:CONSTRAINT\s*`?(\w*)?`?)?\s*UNIQUE\s*(?:KEY|INDEX)?\s*`?(\w*)?`?\s*\((.*?)\)\s*', re.IGNORECASE)
        self.m_fkeys = re.compile(r""",\s*(?:CONSTRAINT\s*`?(\w*)?`?)?\s*FOREIGN\s*KEY\s*(\(?[^\)]*\))?(?:\s*REFERENCES\s*(\w*\(?[^\)]*\)))?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?""", re.IGNORECASE)
        self.m_check=re.compile(r',\s*(?:CONSTRAINT\s*`?(\w*)?`?)?\s*CHECK\s*\(([^()]*\(?[^()]*\)?[^()]*)\)\s*', re.IGNORECASE)
        self.m_inline_pkeys=re.compile(r'(.*?)\bPRIMARY\b\s*\bKEY\b', re.IGNORECASE)
        self.m_inline2_pkeys=re.compile(r'.*,\s*([^,]*?)\bPRIMARY\b\s*\bKEY\b', re.IGNORECASE)

        #re for fields
        self.m_field=re.compile(r'(?:`)?(\w*)(?:`)?\s*(?:`)?(\w*\s*(?:precision|varying)?)(?:`)?\s*((\(\s*\d*\s*\)|\(\s*\d*\s*,\s*\d*\s*\))?)', re.IGNORECASE)
        self.m_dbl_dgt=re.compile(r'((\(\s?\d+\s?),(\s?\d+\s?\)))',re.IGNORECASE)
        self.m_pars=re.compile(r'(\((:?.*?)\))', re.IGNORECASE)
        self.m_dimension=re.compile(r'(\(.*?\))', re.IGNORECASE)
        self.m_fields=re.compile(r'(.*?),', re.IGNORECASE)

        #re for column constraint and auto incremental
        self.m_nulls = re.compile(r'(NOT)?\s*(NULL)', re.IGNORECASE)
        self.m_autoinc = re.compile(r'(AUTO_INCREMENT)', re.IGNORECASE)
        #re for query type
        self.m_rename_table = re.compile(r'(RENAME\s*TABLE)\s*(.*)', re.IGNORECASE)
        self.m_alter_rename_table = re.compile(r'(?:(ALTER\s+?TABLE)\s+(`?\b.*?\b`?))\s+(?:RENAME)\s+(?:TO)?\s+(.*)', re.IGNORECASE)
        self.m_create_table = re.compile(r'(CREATE\s*TABLE)\s*(?:IF\s*NOT\s*EXISTS)?\s*(?:(?:`)?(?:\w*)(?:`)?\.)?(?:`)?(\w*)(?:`)?', re.IGNORECASE)
        self.m_drop_table = re.compile(r'(DROP\s*TABLE)\s*(?:IF\s*EXISTS)?\s*(?:`)?(\w*)(?:`)?', re.IGNORECASE)
        self.m_truncate_table = re.compile(r'(TRUNCATE)\s*(?:TABLE)?\s*(?:(?:`)?(\w*)(?:`)?)(?:.)?(?:`)?(\w*)(?:`)?', re.IGNORECASE)
        self.m_alter_table = re.compile(r'(?:(ALTER\s+?TABLE)\s+(?:`?\b.*?\b`\.?)?(`?\b.*?\b`?))\s+((?:ADD|DROP|CHANGE|MODIFY)\s+(?:\bCOLUMN\b)?.*,?)', re.IGNORECASE)
        self.m_alter_list = re.compile(r'((?:\b(?:ADD|DROP|CHANGE|MODIFY)\b\s+(?:\bCOLUMN\b)?))(.*?,)', re.IGNORECASE)
        self.m_alter_column = re.compile(r'\(?\s*`?(\w*)`?\s*(\w*(?:\s*\w*)?)\s*(?:\((.*?)\))?\)?', re.IGNORECASE)
        self.m_default_value = re.compile(r"(\bDEFAULT\b)\s*('?[\w\.-]*'?)\s*", re.IGNORECASE)
        self.m_alter_change = re.compile(r'\s*`?(\w*)`?\s*`?(\w*)`?\s*(\w*)\s*(?:\((.*?)\))?', re.IGNORECASE)
        self.m_drop_primary = re.compile(r'(?:(?:ALTER\s+?TABLE)\s+(`?\b.*?\b`?)\s+(DROP\s+PRIMARY\s+KEY))', re.IGNORECASE)
        #self.m_modify = re.compile(r'((?:(?:ADD|DROP|CHANGE|MODIFY)\s+(?:\bCOLUMN\b)?))(.*?,)', re.IGNORECASE)
        self.m_ignore_keywords = re.compile(r'(CONSTRAINT)|(PRIMARY)|(INDEX)|(KEY)|(UNIQUE)|(FOREIGN\s*KEY)', re.IGNORECASE)
        self.m_create_full_index = re.compile(
            r"""(CREATE)\s*(UNIQUE|FULLTEXT|SPATIAL)?\s*(INDEX)\s*(\w*)\s*(?:(?:USING)?\s*(BTREE|HASH))?\s*?(?:ON)\s*(\w*)\s*\(\s*(\w*)?(\(?[^\;]*\)?\s*)?\s*\)\s*(KEY_BLOCK_SIZE\s(?:=)\s\w*|(?:USING\s(?:BTREE|HASH))|WITH\sPARSER\s\w*|COMMENT\s*\'[^']*\'|ENGINE_ATTRIBUTE(?:\=)?\'[^']*\'|SECONDARY_ENGINE_ATTRIBUTE(?:\=)?\'[^']*\'|\w*)?\s*(ALGORITHM\s*(?:=)?\s*(?:DEFAULT|INPLACE|COPY)|LOCK\s*(?:=)?\s*(?:DEFAULT|NONE|SHARED|EXCLUSIVE))?\s*""",
            re.IGNORECASE)
        self.m_drop_full_index = re.compile(
            r"""(DROP\s*INDEX)\s*(\w*)\s*(?:ON)\s*(\w*)\s*(ALGORITHM\s*(?:=)?\s*(?:DEFAULT|INPLACE|COPY)|LOCK\s*(?:=)?\s*(?:DEFAULT|NONE|SHARED|EXCLUSIVE))?\s*""",
            re.IGNORECASE)
        #this part t_alter_table aims to match all kind of ALTER TABLE DDL
        self.t_alter_table = re.compile(r"""(ALTER\s*TABLE)\s*(\w*)\s*([^\;]*)""", re.IGNORECASE)
        self.t_ignore_keywords = re.compile(r'(CONSTRAINT)|(FULLTEXT)|(SPATIAL)|(PRIMARY)|(INDEX)|(KEY)|(UNIQUE)|(FOREIGN)', re.IGNORECASE)
        self.t_alter_table_ = []
        # match parse_t_alter_[0]-[4]
        self.t_alter_table_.append(re.compile(r"""(ADD)\s*(?:COLUMN)?\s*\(?\s*(\w*)\s*(\w*(?:\s*\(\s*\w*\s*\))?)\s*(COLLATE\s*\w*)?\s*(GENERATED\s*ALWAYS)?AS\s*(\([^\)]*\))\s*(VIRTUAL|STORED)?\s*(NOT NULL|NULL)?\s*(VISIBLE|INVISIBLE)?\s*(UNIQUE\s*(?:KEY)?)?\s*((?:PRIMARY)?\s*KEY)?\s*(COMMENT\s*\'\w*\')?\s*(REFERENCES\s*\w*\s*\([^\)]*\))?\s*(MATCH\s*FULL|MATCH\s*PARTIAL|MATCH\s*SIMPLE)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\)?\s*((CONSTRAINT\s*\w*)?\s*CHECK\s*\([^\)]*\)\s*((?:NOT)?\s*ENFORCED)?)?\s*(?:FIRST)?(?:AFTER\s*\w*)?""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(ADD)\s*(?:COLUMN)?\s*\(?\s*(\w*)\s*(\w*(?:\s*\(\s*\w*\s*\))?)\s*(NOT NULL|NULL)?\s*(DEFAULT \s*(?:\w*|(?:\([^\)]*\)))?)?\s*(VISIBLE|INVISIBLE)?\s*(AUTO_INCREMENT)?\s*(UNIQUE\s*(?:KEY)?)?\s*((?:PRIMARY)?\s*KEY)?\s*(COMMENT\s*\'\w*\')?\s*(COLLATE\s*\w*)?\s*(COLUMN_FORMAT\s*(?:FIXED|DYNAMIC|DEFAULT))?\s*((?:ENGINE_ATTRIBUTE|SECONDARY_ENGINE_ATTRIBUTE)\s*\=\s*\'\w*\')?\s*(STORAGE\s*(?:DISK|MEMORY))?\s*(REFERENCES\s*\w*\s*\([^\)]*\))?\s*(MATCH\s*FULL|MATCH\s*PARTIAL|MATCH\s*SIMPLE)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\)?\s*((CONSTRAINT\s*\w*)?\s*CHECK\s*\([^\)]*\)\s*((?:NOT)?\s*ENFORCED)?)?\s*(FIRST)?(AFTER\s*\w*)?""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(ADD)\s*(INDEX|KEY)\s*(\w*)?\s*(USING\s*(?:BTREE|HASH))?\s*((?:\(\s*\w*\s*\(\s*.*?\s*\)\s*\w*\s*\))|(?:\(\s*.*?\s*\)))\s*((KEY_BLOCK_SIZE(?:\=)?\s*\w*\s*)|(USING\s*(?:BTREE|HASH)\s*)|(WITH\s*PARSER\s*\w*\s*)|(COMMENT\s*\'\w*\'\s*)|(VISIBLE\s*|INVISIBLE\s*)){0,5}""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(ADD)\s*(FULLTEXT|SPATIAL)\s*(INDEX|KEY)?\s*(\w*)?\s*(\(.*?\))\s*((KEY_BLOCK_SIZE(?:\=)?\s*\w*\s*)|(USING\s*(?:BTREE|HASH)\s*)|(WITH\s*PARSER\s*\w*\s*)|(COMMENT\s*\'\w*\'\s*)|(VISIBLE\s*|INVISIBLE\s*)){0,5}""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(ADD)\s*(CONSTRAINT\s*(\w*)?)?\s*(PRIMARY\s*KEY)\s*(USING\s*(?:BTREE|HASH)\s*)?\s*(\(.*?\))\s*((KEY_BLOCK_SIZE(?:\=)?\s*\w*\s*)|(USING\s*(?:BTREE|HASH)\s*)|(WITH\s*PARSER\s*\w*\s*)|(COMMENT\s*\'\w*\'\s*)|(VISIBLE\s*|INVISIBLE\s*)){0,5}""", re.IGNORECASE))
        # match parse_t_alter_[5]-[9]
        self.t_alter_table_.append(re.compile(r"""(ADD)\s*(CONSTRAINT\s*(\w*)?)?\s*(UNIQUE\s*(INDEX|KEY)?)\s*\`?(\w*)?\`?\s*(USING\s*(?:BTREE|HASH)\s*)?\s*(\(.*?\))\s*((KEY_BLOCK_SIZE(?:\=)?\s*\w*\s*)|(USING\s*(?:BTREE|HASH)\s*)|(WITH\s*PARSER\s*\w*\s*)|(COMMENT\s*\'\w*\'\s*)|(VISIBLE\s*|INVISIBLE\s*)){0,5}""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(ADD)\s*(CONSTRAINT\s*(\w*)?)?\s*(FOREIGN\s*KEY)\s*(\w*)?\s*(\(.*?\))\s*(REFERENCES\s*\w*\s*(?:\(.*?\)))?\s*(MATCH\s*FULL|MATCH\s*PARTIAL|MATCH\s*SIMPLE)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(ADD)\s*(CONSTRAINT\s*(\w*)?)?\s*(CHECK)\s*(\(.*?\))\s*((?:NOT\s*)?ENFORCED)?""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(DROP)\s*(CHECK|CONSTRAINT)\s*(\w*)""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(ALTER)\s*(CHECK|CONSTRAINT)\s*(\w*)\s*(NOT)?\s*ENFORCED""", re.IGNORECASE))
        # match parse_t_alter_[10]-[14]
        self.t_alter_table_.append(re.compile(r"""(ALGORITHM)\s*(\=)?\s*(DEFAULT|INSTANT|INPLACE|COPY)""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(ALTER)\s*(COLUMN)?\s*(\w*)\s*((?:SET\s*DEFAULT\s*\(.*?\))|(?:SET\s*DEFAULT\s*(?:\w*))|SET\s*(?:VISIBLE|INVISIBLE)|DROP\s*DEFAULT)""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(ALTER)\s*(INDEX)\s*(\w*)\s*(VISIBLE|INVISIBLE)""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(CHANGE)\s*(COLUMN)?\s*(\w*)\s*(\w*)\s*(\w*)\s*(COLLATE\s*\w*)?\s*(GENERATED\s*ALWAYS)?AS\s*(\([^\)]*\))\s*(VIRTUAL|STORED)?\s*(NOT NULL|NULL)?\s*(VISIBLE|INVISIBLE)?\s*(UNIQUE\s*(?:KEY)?)?\s*((?:PRIMARY)?\s*KEY)?\s*(COMMENT\s*\'\w*\')?\s*(REFERENCES\s*\w*\s*\([^\)]*\))?\s*(MATCH\s*FULL|MATCH\s*PARTIAL|MATCH\s*SIMPLE)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*((CONSTRAINT\s*\w*)?\s*CHECK\s*\([^\)]*\)\s*((?:NOT)?\s*ENFORCED)?)?\s*(?:FIRST)?(?:AFTER\s*\w*)?""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(CHANGE)\s*(COLUMN)?\s*(\w*)\s*(\w*)\s*(\w*)\s*(NOT NULL|NULL)?\s*(DEFAULT \s*(?:\w*|(?:\([^\)]*\)))?)?\s*(VISIBLE|INVISIBLE)?\s*(AUTO_INCREMENT)?\s*(UNIQUE\s*(?:KEY)?)?\s*((?:PRIMARY)?\s*KEY)?\s*(COMMENT\s*\'\w*\')?\s*(COLLATE\s*\w*)?\s*(COLUMN_FORMAT\s*(?:FIXED|DYNAMIC|DEFAULT))?\s*((?:ENGINE_ATTRIBUTE|SECONDARY_ENGINE_ATTRIBUTE)\s*\=\s*\'\w*\')?\s*(STORAGE\s*(?:DISK|MEMORY))?\s*(REFERENCES\s*\w*\s*\([^\)]*\))?\s*(MATCH\s*FULL|MATCH\s*PARTIAL|MATCH\s*SIMPLE)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*((CONSTRAINT\s*\w*)?\s*CHECK\s*\([^\)]*\)\s*((?:NOT)?\s*ENFORCED)?)?\s*(FIRST)?(AFTER\s*\w*)?""", re.IGNORECASE))
        # match parse_t_alter_[15]-[19]
        self.t_alter_table_.append(re.compile(r"""(DEFAULT)?\s*(CHARACTER)\s*(SET)\s*(\=)?\s*(\w*)\s*(COLLATE\s*(?:\=)?\s*\w*)?""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(CONVERT\s*TO\s*CHARACTER\s*SET)\s*(\w*)\s*(COLLATE\s*\w*)?""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(DISABLE|ENABLE)\s*KEYS""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(DISCARD|IMPORT)\s*TABLESPACE""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(DROP)\s*(INDEX|KEY)\s*(\w*)""", re.IGNORECASE))
        # match parse_t_alter_[20]-[24]
        self.t_alter_table_.append(re.compile(r"""(DROP)\s*PRIMARY\s*KEY""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(DROP)\s*FOREIGN\s*KEY\s*(\w*)""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(DROP)\s*(COLUMN)?\s*(\w*)""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""FORCE\s*""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(LOCK)\s*(\=)?\s*(DEFAULT|NONE|SHARED|EXCLUSIVE)\s*""", re.IGNORECASE))
        # match parse_t_alter_[25]-[29]
        self.t_alter_table_.append(re.compile(r"""(MODIFY)\s*(COLUMN)?\s*(\w*)\s*(\w*)\s*(COLLATE\s*\w*)?\s*(GENERATED\s*ALWAYS)?AS\s*(\([^\)]*\))\s*(VIRTUAL|STORED)?\s*(NOT NULL|NULL)?\s*(VISIBLE|INVISIBLE)?\s*(UNIQUE\s*(?:KEY)?)?\s*((?:PRIMARY)?\s*KEY)?\s*(COMMENT\s*\'\w*\')?\s*(REFERENCES\s*\w*\s*\([^\)]*\))?\s*(MATCH\s*FULL|MATCH\s*PARTIAL|MATCH\s*SIMPLE)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*((CONSTRAINT\s*\w*)?\s*CHECK\s*\([^\)]*\)\s*((?:NOT)?\s*ENFORCED)?)?\s*(?:FIRST)?(?:AFTER\s*\w*)?""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(MODIFY)\s*(COLUMN)?\s*(\w*)\s*(\w*)\s*(NOT NULL|NULL)?\s*(DEFAULT \s*(?:\w*|(?:\([^\)]*\)))?)?\s*(VISIBLE|INVISIBLE)?\s*(AUTO_INCREMENT)?\s*(UNIQUE\s*(?:KEY)?)?\s*((?:PRIMARY)?\s*KEY)?\s*(COMMENT\s*\'\w*\')?\s*(COLLATE\s*\w*)?\s*(COLUMN_FORMAT\s*(?:FIXED|DYNAMIC|DEFAULT))?\s*((?:ENGINE_ATTRIBUTE|SECONDARY_ENGINE_ATTRIBUTE)\s*\=\s*\'\w*\')?\s*(STORAGE\s*(?:DISK|MEMORY))?\s*(REFERENCES\s*\w*\s*\([^\)]*\))?\s*(MATCH\s*FULL|MATCH\s*PARTIAL|MATCH\s*SIMPLE)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*(ON\s*(?:DELETE|UPDATE)\s*(?:RESTRICT\s*|CASCADE\s*|SET\s*NULL\s*|NO\s*ACTION\s*|SET\s*DEFAULT\s*)?)?\s*((CONSTRAINT\s*\w*)?\s*CHECK\s*\([^\)]*\)\s*((?:NOT)?\s*ENFORCED)?)?\s*(FIRST)?(AFTER\s*\w*)?""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(ORDER)\s*(BY)\s*([^\;]*)""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(RENAME)\s*(COLUMN)\s*(\w*)\s*(TO)\s*(\w*)\s*""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(RENAME)\s*(INDEX|KEY)\s*(\w*)\s*(TO)\s*(\w*)\s*""", re.IGNORECASE))
        # match parse_t_alter_[30]-[31]
        self.t_alter_table_.append(re.compile(r"""(RENAME)\s*(TO|AS)?\s*(\w*)\s*""", re.IGNORECASE))
        self.t_alter_table_.append(re.compile(r"""(WITHOUT|WITH)\s*VALIDATION\s*""", re.IGNORECASE))

        #adding partition table
        self.t_par_table = re.compile(r"""PARTITION\s*BY\s*((?:(?:LINEAR)?\s*HASH\s*\([\s\w]*\(?[\s\w]*\)?\s*\)\s*)|(?:(?:LINEAR)?\s*KEY\s*(?:ALGORITHM\s*\=\s*\w*)?\s*\([^\)]*\)\s*)|(?:RANGE\s*(?:COLUMNS)?\s*\([^\)]*\)\s*)|(?:LIST\s*(?:COLUMNS)?\s*\([^\)]*\)\s*))\s*(?:PARTITIONS\s*(\w*))?\s*\(?([^\;]*)\)?""", re.IGNORECASE)
        self.t_par_sub = re.compile(r"""SUBPARTITION\s*BY\s*((?:(?:LINEAR)?\s*HASH\s*\([^\)]*(?:[\s\)]*)?\)\s*)|(?:(?:LINEAR)?\s*KEY\s*(?:ALGORITHM\s*\=\s*\w*)?\s*\([^\)]*\)\s*))\s*(?:SUBPARTITIONS\s*(\w*))?\s*\(([^\;]*)\)""", re.IGNORECASE)
        self.t_par_def = re.compile(r"""PARTITION\s*(\w*)\s*(?:(?:VALUES\s*LESS\s*THAN\s*\(?\s*([\w\,\s]*)\s*\)?)|(?:VALUES\s*IN\s*\(\s*(.*?)\s*\)))?\s*(?:TABLESPACE\s*\=?\s*(\w*))?\s*""", re.IGNORECASE)
        self.t_alter_part = re.compile(r"""ALTER\s*TABLE\s*(\w*)\s*(\w*)\s*PARTITION\s*([\w\,\s]*)\s*(?:\(([^\;]*)\))?""", re.IGNORECASE)
        self.expression = re.compile(r"""\(([\s\w]*\(?[\s\w\,]*\)?\s*)\)""", re.IGNORECASE)
    VERSION_SCALE = 1000

    def reset_lists(self):
        """
            The method resets the lists to empty lists after a successful tokenisation.
        """
        self.tokenised=[]
        self.query_list=[]

    def parse_column(self, col_def):
        """
            This method parses the column definition searching for the name, the data type and the
            dimensions.
            If there's a match the dictionary is built with the keys
            column_name, the column name
            data_type, the column's data type
            is nullable, the value is set always to yes except if the column is primary key ( column name present in key_cols)
            enum_list,character_maximum_length,numeric_precision are the dimensions associated with the data type.
            The auto increment is set if there's a match for the auto increment specification.s

            :param col_def: The column definition
            :return: col_dic the column dictionary
            :rtype: dictionary
        """
        colmatch = self.m_field.search(col_def)
        dimmatch = self.m_dimension.search(col_def)
        col_dic={}
        dimensions = None
        if colmatch:
            col_dic["column_name"]=colmatch.group(1).strip("`").strip()
            col_dic["data_type"]=colmatch.group(2).lower().strip()
            col_dic["is_nullable"]="YES"
            if dimmatch:
                dimensions = dimmatch.group(1).replace('|', ',').replace('(', '').replace(')', '').strip()
                enum_list = dimmatch.group(1).replace('|', ',').strip()
                numeric_dims = dimensions.split(',')
                numeric_precision = numeric_dims[0].strip()
                try:
                    numeric_scale = numeric_dims[1].strip()
                except:
                    numeric_scale = "None"

                col_dic["enum_list"] = enum_list
                col_dic["character_maximum_length"] = dimensions
                col_dic["numeric_precision"]=numeric_precision
                col_dic["numeric_scale"]=numeric_scale
            elif col_dic["data_type"] in ["decimal", "dec", "numeric", "fixed"]:
                col_dic["numeric_precision"] = DEFAULT_NUMERIC_PRECISION
                col_dic["numeric_scale"] = DEFAULT_NUMERIC_SCALE
                dimensions = "%s,%s" % (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
            nullcons = self.m_nulls.search(col_def)
            autoinc = self.m_autoinc.search(col_def)
            default_value = self.m_default_value.search(col_def)
            pkey_list = self.pkey_cols

            col_dic["is_nullable"]="YES"
            if col_dic["column_name"] in pkey_list or col_dic["column_name"] in self.ukey_cols:
                col_dic["is_nullable"]="NO"
            elif nullcons:
                #pkey_list=[cln.strip() for cln in pkey_list]
                if nullcons.group(0).upper()=="NOT NULL":
                    col_dic["is_nullable"]="NO"

            if autoinc:
                col_dic["extra"] = "auto_increment"
            else:
                col_dic["extra"] = ""
            if default_value:
                col_dic["default"] = default_value.group(2)
            else:
                col_dic["default"] = None
            if dimensions:
                col_dic["column_type"] = "%s(%s)" % (col_dic["data_type"], dimensions)
            else:
                col_dic["column_type"] = "%s" % (col_dic["data_type"])

        return col_dic

    def quote_cols(self, cols):
        """
            The method adds the " quotes to the column names.
            The string is converted to a list using the split method with the comma separator.
            The columns are then stripped and quoted with the "".
            Finally the list elements are rejoined in a string which is returned.
            The method is used in build_key_dic to sanitise the column names.

            :param cols: The columns string
            :return: The columns quoted between ".
            :rtype: text
        """
        idx_cols = cols.split(',')
        idx_cols = ['"%s"' % col.strip() for col in idx_cols]
        quoted_cols = ",".join(idx_cols)
        return quoted_cols


    def build_key_dic(self, inner_stat, table_name, column_list):
        """
            The method matches and tokenise the primary key and index/key definitions in the create table's inner statement.

            As the primary key can be defined as column or table constraint there is an initial match attempt with the regexp m_inline_pkeys.
            If the match is successful then the primary key dictionary is built from the match data.
            Otherwise the primary key dictionary is built using the eventual table key definition.

            The method search for primary keys keys and indices defined in the inner_stat.
            The index name PRIMARY is used to tell pg_engine we are building a primary key.
            Otherwise the index name is built using the format (uk)idx_tablename[0:20] + counter.
            If there's a match for a primary key the composing columns are saved into pkey_cols.

            The tablename limitation is required as PostgreSQL enforces a strict limit for the identifier name's lenght.

            Each key dictionary have three keys.
            index_name, the index name or PRIMARY
            index_columns, a list with the column names
            non_unique, follows the MySQL's information schema convention and marks an index if is unique or not.

            When the dictionary is built is appended to idx_list and finally returned to the calling method parse_create_table.s


            :param inner_stat: The statement within the round brackets in CREATE TABLE
            :param table_name: The table name
            :return: idx_list the list of dictionary with the index definitions
            :rtype: list
        """
        key_dic={}
        idx_list=[]
        idx_counter=0
        inner_stat= "%s," % inner_stat.strip()
        inner_stat = ' '.join(inner_stat.split()).replace("USING BTREE","").replace("USING HASH","")
        inner_stat = inner_stat.replace("using btree","").replace("using hash","")

        pk_match =  self.m_inline_pkeys.match(inner_stat)
        pk_match2 = self.m_inline2_pkeys.match(inner_stat)
        pkey=self.m_pkeys.findall(inner_stat)

        ukey=self.m_ukeys.findall(inner_stat)
        idx=self.m_idx.findall(inner_stat)
        idx2=self.m_idx_2.findall(inner_stat)
        fkey=self.m_fkeys.findall(inner_stat)
        ckey=self.m_check.findall(inner_stat)
        #need to change the type
        if pk_match:
            key_dic["index_name"] = 'PRIMARY'
            key_dic["index_n"] = 'PRIMARY'
            if pkey:
                key_dic["constraint_name"] = pkey[0][0]
                index_columns = {pkey[0][1]}
            elif pk_match2:
                index_columns = ((pk_match2.group(1).strip().split()[0]).replace('`', '')).split(',')
            else:
                index_columns = ((pk_match.group(1).strip().split()[0]).replace('`', '')).split(',')
            idx_cols = [(column.strip().split()[0]).replace('`', '') for column in index_columns if column.strip() != '']
            key_dic["index_columns"] = idx_cols
            key_dic["non_unique"]=0
            self.pkey_cols = idx_cols
            idx_list.append(dict(list(key_dic.items())))
            key_dic={}
        if ukey:
            for cols in ukey:
                if cols[0]:
                    name_tmp = cols[0]
                elif cols[1]:
                    name_tmp = cols[1]
                else:
                    name_tmp = cols[2]
                key_dic["index_name"] = name_tmp.replace('`', '').strip()
                key_dic["index_n"] = 'UNIQUE'
                index_columns = cols[2].strip().split(',')
                idx_cols = [(column.strip().split()[0]).replace('`', '') for column in index_columns if column.strip() != '']
                key_dic["index_columns"] = idx_cols
                key_dic["non_unique"]=0
                idx_list.append(dict(list(key_dic.items())))
                key_dic={}
                idx_counter+=1
                self.ukey_cols = self.ukey_cols+[column for column in idx_cols if column not in self.ukey_cols]
        if idx:
            for cols in idx:
                if cols[1]:
                    key_dic["index_name"] = cols[0].replace('`', '').strip()
                else:
                    key_dic["index_name"] = 'idx_' + table_name[0:20] + '_' + str(idx_counter)
                key_dic["index_n"] = 'INDEX'
                index_columns = cols[1].strip().split(',')
                idx_cols = [(column.strip().split()[0]).replace('`', '') for column in index_columns if column.strip() != '']
                key_dic["index_columns"] = idx_cols
                key_dic["non_unique"]=1
                idx_list.append(dict(list(key_dic.items())))
                key_dic={}
                idx_counter+=1
        if idx2:
            for cols in idx:
                if cols[1]:
                    key_dic["index_name"] = cols[0].replace('`', '').strip()
                else:
                    key_dic["index_name"] = 'idx_' + table_name[0:20] + '_' + str(idx_counter)
                key_dic["index_n"] = 'INDEX'
                index_columns = cols[1].strip().split(',')
                idx_cols = [(column.strip().split()[0]).replace('`', '') for column in index_columns if column.strip() != '']
                key_dic["index_columns"] = idx_cols
                key_dic["non_unique"]=1
                idx_list.append(dict(list(key_dic.items())))
                key_dic={}
                idx_counter+=1
        if fkey:
            for cols in fkey:
                key_dic["constraint_name"] = cols[0]
                key_dic["index_name"] = table_name+'_ibfk'
                key_dic["index_n"] = 'FOREIGN'
                key_dic["index_columns"] = cols[1].replace("(","").replace(")","").strip()
                key_dic["references_match"] = cols[2]
                key_dic["references_on"] = (cols[3].upper()+" "+cols[4].upper()).strip()
                idx_list.append(dict(list(key_dic.items())))
                key_dic={}
                idx_counter+=1
        if ckey:
            for cols in ckey:
                key_dic["constraint_name"] = cols[0]
                key_dic["index_name"] = table_name+"_chk"
                key_dic["index_n"] = 'CHECK'
                key_dic["index_columns"] = cols[1]
                key_dic["non_unique"] = 1
                idx_list.append(dict(list(key_dic.items())))
                key_dic = {}
                idx_counter += 1
        column_key =self.m_fields.findall(column_list+',')
        comments = []
        for col_def in column_key:
            col_def=col_def.strip()
            com = self.m_comment.findall(col_def)
            ukey = col_def.upper().find("UNIQUE")
            ckey = col_def.upper().find("CHECK")
            if ukey!=-1:
                col_match = self.m_field.search(col_def)
                key_dic["index_name"] = col_match.group(1).strip()
                key_dic["index_n"] = 'UNIQUE'
                index_columns = [col_match.group(1).strip()]
                idx_cols = [(column.strip().split()[0]).replace('`', '') for column in index_columns if
                            column.strip() != '']
                key_dic["index_columns"] = idx_cols
                key_dic["non_unique"] = 0
                idx_list.append(dict(list(key_dic.items())))
                key_dic = {}
                idx_counter += 1
                self.ukey_cols = self.ukey_cols+[column for column in idx_cols if column not in self.ukey_cols]
            if ckey!=-1:
                check = col_def.replace("check", ",check").replace("CHECK", ",CHECK")
                ckey = self.m_check.findall(check)
                key_dic["constraint_name"] = ""
                key_dic["index_name"] = table_name + "_chk"
                key_dic["index_n"] = 'CHECK'
                key_dic["index_columns"] = ckey[0][1].strip()
                key_dic["non_unique"] = 1
                idx_list.append(dict(list(key_dic.items())))
                key_dic = {}
                idx_counter += 1
            if com:
                col_match = self.m_field.search(col_def)
                com_dic = {}
                com_dic["type"] = "column"
                com_dic["name"] = col_match.group(1).strip()
                com_dic["comment"] = com[0]
                comments.append(com_dic)
        return [comments, idx_list]

    def build_column_dic(self, inner_stat):
        """
            The method builds a list of dictionaries with the column definitions.

            The regular expression m_fields is used to find all the column occurrences and, for each occurrence,
            the method parse_column is called.
            If parse_column returns a dictionary, this is appended to the list col_parse.

            :param inner_stat: The statement within the round brackets in CREATE TABLE
            :return: cols_parse the list of dictionary with the column definitions
            :rtype: list
        """
        column_list=self.m_fields.findall(inner_stat)
        cols_parse=[]
        for col_def in column_list:
            col_def=col_def.strip()
            col_dic=self.parse_column(col_def)
            if col_dic:
                cols_parse.append(col_dic)
        return cols_parse

    def parse_partition(self, sql):
        """
        Split the partition table into three parts for parsing
        And generate partition table metadata according to different situations
        partition_options:
        PARTITION BY
            { [LINEAR] HASH(expr)
            | [LINEAR] KEY [ALGORITHM={1 | 2}] (column_list)
            | RANGE{(expr) | COLUMNS(column_list)}
            | LIST{(expr) | COLUMNS(column_list)} }
        [PARTITIONS num]

        [SUBPARTITION BY
            { [LINEAR] HASH(expr)
            | [LINEAR] KEY [ALGORITHM={1 | 2}] (column_list) }
          [SUBPARTITIONS num]
        ]
        [(partition_definition [, partition_definition] ...)]
        par_item:
            [0]:partition_name/subpartition_name
            [1]:VALUES LESS THAN
            [2]:VALUES IN
            [3]:TABLESPACE
        """
        stat_dic = []
        par_cmd = []
        count = 0
        scount = 0
        # partition by
        par_stat = self.t_par_table.match(sql)
        if par_stat is None:
            return []
        par_method = par_stat.group(1).strip()
        partition_method = par_method[:par_method.find("(")].strip()
        partition_expression = self.expression.search(par_method).group(1).strip()
        if partition_expression.find('(') != -1:
            print("Now we do not support the expression expr as the key value partitions_keys")
            partition_expression = self.expression.search(partition_expression).group(1).strip()
        partition_number = par_stat.group(2)
        # subpartition by
        par_stat_1 = par_stat.group(3).strip()
        par_sub = self.t_par_sub.match(par_stat_1)
        if par_sub:
            subpar_method = par_sub.group(1).strip()
            subpartition_method = subpar_method[:subpar_method.find("(")].strip()
            subpartition_expression = self.expression.search(subpar_method).group(1).strip()
            if subpartition_expression.find('(') != -1:
                print("Now we do not support the expression expr as the key value partitions_keys")
                subpartition_expression = self.expression.search(subpartition_expression).group(1).strip()
            subpartition_number = par_sub.group(2)
            par_stat_2 = par_sub.group(3).strip()
            par_def = self.t_par_def.findall(par_stat_2)
            partition_name = ""
            partition_description = ""
            for par_item in par_def:
                par_dic = {}
                partition_string = ' '.join(par_stat_2.upper().split())
                find_string = "SUBPARTITION "+par_item[0].upper()
                if partition_string.find(find_string+" ")==-1 and partition_string.find(find_string+",")==-1:
                    #partition
                    count += 1
                    scount = 0
                    partition_name = par_item[0]
                    partition_description = par_item[1]+par_item[2]
                else:
                    # subpartition
                    scount +=1
                    subpartition_name = par_item[0]
                    par_dic = {}
                    par_dic["partition_ordinal_position"] = count
                    par_dic["subpartition_ordinal_position"] = scount
                    par_dic["subpartition_name"] = subpartition_name
                    par_dic["subpartition_method"] = subpartition_method.upper()
                    par_dic["subpartition_expression"] = subpartition_expression
                    par_dic["partition_name"] = partition_name
                    par_dic["partition_method"] = partition_method.upper()
                    par_dic["partition_expression"] = partition_expression
                    par_dic["partition_description"] = partition_description
                    par_dic["tablespace_name"] = par_item[3]
                    par_cmd.append(par_dic)
                    stat_dic = par_cmd
                if subpartition_number:
                    for i in range(0, int(subpartition_number)):
                        scount += 1
                        subpartition_name = "p%dsp%d" % (count, scount-1)
                        par_dic = {}
                        par_dic["partition_ordinal_position"] = count
                        par_dic["subpartition_ordinal_position"] = scount
                        par_dic["subpartition_name"] = subpartition_name
                        par_dic["subpartition_method"] = subpartition_method.upper()
                        par_dic["subpartition_expression"] = subpartition_expression
                        par_dic["partition_name"] = partition_name
                        par_dic["partition_method"] = partition_method.upper()
                        par_dic["partition_expression"] = partition_expression
                        par_dic["partition_description"] = partition_description
                        par_dic["tablespace_name"] = par_item[3]
                        par_cmd.append(par_dic)
                        stat_dic = par_cmd
            return stat_dic
        else:
            subpartition_method = ""
            subpartition_expression = ""
            par_stat_2 = par_stat_1
        # partition_definition
        par_def = self.t_par_def.findall(par_stat_2)
        for par_item in par_def:
            par_dic = {}
            count += 1
            par_dic["partition_ordinal_position"] = count
            par_dic["subpartition_method"] = subpartition_method.upper()
            par_dic["partition_name"] = par_item[0]
            par_dic["partition_method"] = partition_method.upper()
            par_dic["partition_expression"] = partition_expression
            par_dic["partition_description"] = par_item[1]+par_item[2]
            par_cmd.append(par_dic)
            stat_dic = par_cmd
        #hash with partition_number
        if partition_number :
            for i in range(int(partition_number)):
                par_dic = {}
                par_dic["partition_ordinal_position"] = i+1
                par_dic["subpartition_method"] = subpartition_method.upper()
                par_dic["partition_name"] = "p"+str(i)
                par_dic["partition_method"] = partition_method.upper()
                par_dic["partition_expression"] = partition_expression
                par_dic["partition_description"] = ""
                stat_dic.append(par_dic)
        # hash without partition_number
        if stat_dic == []:
            par_dic = {}
            par_dic["partition_ordinal_position"] = 1
            par_dic["subpartition_method"] = subpartition_method.upper()
            par_dic["partition_name"] = "p0"
            par_dic["partition_method"] = partition_method.upper()
            par_dic["partition_expression"] = partition_expression
            par_dic["partition_description"] = ""
            stat_dic.append(par_dic)
        return stat_dic

    def parse_alter_partition(self, sql):
        """
        This part is to parse alter table with partition
        """
        partition = {}
        par_alt = []
        par_stat = self.t_alter_part.findall(sql)
        for par_item in par_stat:
            par_dic = {}
            par_dic["tbl_name"] = par_item[0]
            command = par_item[1].upper()
            par_dic["command"] = command
            if command == "ADD":
                par_dic["part"] = par_item[3].replace("MAXVALUE", "(MAXVALUE)").replace("maxvalue", "(MAXVALUE)").replace(" PARTITION ", " ADD PARTITION ").replace(
                    " partition ", " ADD PARTITION ").replace("IN", "").replace("in", "").strip()
            elif command == "DROP":
                par_dic["part"] = par_item[2].replace(" ", "").split(',')
            elif command == "TRUNCATE":
                par_dic["part"] = par_item[2].replace(" ", "").split(',')
            elif command == "COALESCE":
                par_dic["part"] = par_item[2].replace(" ", "").split(',')
            elif command == "REORGANIZE":
                par_dic["part1"] = par_item[2].replace(" ","").replace("INTO","").replace("into", "").split(",")
                part2 = self.t_par_def.findall(par_item[3])
                par_dic["part2"] = []
                for part2_item in part2:
                    try:
                        par_dic["part2"].append(part2_item)
                    except:
                        par_dic["part2"] = []
            elif command == "EXCHANGE":
                part_tmp = par_item[2].replace("WITH","with").replace("TABLE","table")
                par_dic["part1"] = part_tmp[0:part_tmp.find("with")].strip()
                par_dic["part2"] = part_tmp[part_tmp.find("table"):].replace("table","").strip()
            else:
                print("The statement is not supported")
            par_alt.append(par_dic)
            partition["par_alt"] = par_alt
        return partition


    def parse_create_table(self, sql_create, table_name):
        """
            The method parse and generates a dictionary from the CREATE TABLE statement.
            The regular expression m_inner is used to match the statement within the round brackets.

            This inner_stat is then cleaned from the primary keys, keys indices and foreign keys in order to get
            the column list.
            The indices are stored in the dictionary key "indices" using the method build_key_dic.
            The regular expression m_pars is used for finding and replacing all the commas with the | symbol within the round brackets
            present in the columns list.
            At the column list is also appended a comma as required by the regepx used in build_column_dic.
            The build_column_dic method is then executed and the return value is stored in the dictionary key "columns"

            :param sql_create: The sql string with the CREATE TABLE statement
            :param table_name: The table name
            :return: table_dic the table dictionary tokenised from the CREATE TABLE
            :rtype: dictionary
        """

        m_inner = self.m_inner.search(sql_create)
        inner_stat = m_inner.group(1).strip()
        table_dic = {}

        column_list = self.m_pkeys.sub( '', inner_stat)
        column_list = self.m_ukeys.sub( '', column_list)
        column_list = self.m_idx.sub( '', column_list)
        column_list = self.m_idx_2.sub( '', column_list)
        column_list = self.m_fkeys.sub('', column_list)
        column_list = self.m_check.sub('', column_list)
        [table_dic["comment"], table_dic["indices"]] = self.build_key_dic(inner_stat, table_name, column_list)
        mpars  =self.m_pars.findall(column_list)
        for match in mpars:
            new_group=str(match[0]).replace(',', '|')
            column_list=column_list.replace(match[0], new_group)
        column_list=column_list+","
        other = sql_create[sql_create.find(m_inner.group(0)):].replace(m_inner.group(0), "").strip()
        com = self.m_comment.findall(other)
        if com!=[]:
            com_dic = {}
            com_dic["type"] = "table"
            com_dic["name"] = table_name
            com_dic["comment"] = com[0]
            table_dic["comment"].append(com_dic)
        table_dic["columns"]=self.build_column_dic(column_list)
        return table_dic

    def parse_alter_table(self, malter_table):
        """
            The method parses the alter table match.
            As alter table can be composed of multiple commands the original statement (group 0 of the match object)
            is searched with the regexp m_alter_list.
            For each element in returned by findall the first word is evaluated as command. The parse alter table
            manages the following commands.
            DROP,ADD,CHANGE,MODIFY.

            Each command build a dictionary alter_dic with at leaset the keys command and name defined.
            Those keys are respectively the commant itself and the attribute name affected by the command.

            ADD defines the keys type and dimension. If type is enum then the dimension key stores the enumeration list.

            CHANGE defines the key command and then runs a match with m_alter_change. If the match is successful
            the following keys are defined.

            old is the old previous field name
            new is the new field name
            type is the new data type
            dimension the field's dimensions or the enum list if type is enum

            MODIFY works similarly to CHANGE except that the field is not renamed.
            In that case we have only the keys type and dimension defined along with name and command.s

            The class's regular expression self.m_ignore_keywords is used to skip the CONSTRAINT,INDEX and PRIMARY and FOREIGN KEY KEYWORDS in the
            alter command.

            :param malter_table: The match object returned by the match method against tha alter table statement.
            :return: stat_dic the alter table dictionary tokenised from the match object.
            :rtype: dictionary
        """
        stat_dic={}
        alter_cmd=[]
        alter_stat=malter_table.group(0) + ','
        stat_dic["command"]=malter_table.group(1).upper().strip()
        stat_dic["name"]=malter_table.group(2).strip().strip('`')
        dim_groups=self.m_dimension.findall(alter_stat)

        for dim_group in dim_groups:
            alter_stat=alter_stat.replace(dim_group, dim_group.replace(',','|'))

        alter_list=self.m_alter_list.findall(alter_stat)
        for alter_item in alter_list:
            alter_dic={}
            m_ignore_item = self.m_ignore_keywords.search(alter_item[1])

            if not m_ignore_item:
                command = (alter_item[0].split())[0].upper().strip()
                if command == 'DROP':
                    alter_dic["command"] = command
                    alter_dic["name"] = alter_item[1].strip().strip(',').replace('`', '').strip()
                elif command == 'ADD':
                    alter_string = alter_item[1].strip()

                    alter_column=self.m_alter_column.search(alter_string)
                    default_value = self.m_default_value.search(alter_string)
                    if alter_column:

                        column_type = alter_column.group(2).lower().strip()

                        alter_dic["command"] = command
                        # this is a lesser horrible hack, still needs to be improved
                        alter_dic["name"] = alter_column.group(1).strip().strip('`')
                        alter_dic["type"] = column_type.split(' ')[0]
                        try:
                            alter_dic["dimension"]=alter_column.group(3).replace('|', ',').strip()
                        except:
                            alter_dic["dimension"]=0
                        if default_value:
                            alter_dic["default"] = default_value.group(2)
                        else:
                            alter_dic["default"] = None

                elif command == 'CHANGE':
                    alter_dic["command"] = command
                    alter_column = self.m_alter_change.search(alter_item[1].strip())
                    if alter_column:
                        alter_dic["command"] = command
                        alter_dic["old"] = alter_column.group(1).strip().strip('`')
                        alter_dic["new"] = alter_column.group(2).strip().strip('`')
                        alter_dic["type"] = alter_column.group(3).strip().strip('`').lower()
                        alter_dic["name"] = alter_column.group(1).strip().strip('`')
                        try:
                            alter_dic["dimension"]=alter_column.group(4).replace('|', ',').strip()
                        except:
                            alter_dic["dimension"]=0

                elif command == 'MODIFY':
                    alter_string = alter_item[1].strip()

                    alter_column = self.m_alter_column.search(alter_string)
                    if alter_column:
                        alter_dic["command"] = command
                        alter_dic["name"] = alter_column.group(1).strip().strip('`')
                        # this is a lesser horrible hack, still needs to be improved
                        column_type = alter_column.group(2).lower().strip()
                        alter_dic["type"] = column_type.split(' ')[0]
                        try:
                            alter_dic["dimension"]=alter_column.group(3).replace('|', ',').strip()
                        except:
                            alter_dic["dimension"] = 0
                if command != 'DROP':
                    alter_dic["data_type"] = alter_dic["type"]
                    if alter_dic["dimension"] == 0:
                        alter_dic["column_type"] = alter_dic["type"]
                    else:
                        alter_dic["column_type"] = "%s(%s)" % (alter_dic["type"], alter_dic["dimension"])
                alter_cmd.append(alter_dic)
            stat_dic["alter_cmd"]=alter_cmd
        return stat_dic

    def parse_t_alter_table(self, talter_table):
        """
        The method parses all ALTER TABLE statement from the token data.
        Covers various types of alter table statements and guide different types of alter table statements to corresponding functions
        from parse_t_alter_0 to parse_t_alter_31, and the parse function parse_t_alter_x is match the build function build_t_alter_x in pg_lib.

        For better understanding please have a look to
            https://dev.mysql.com/doc/refman/8.0/en/alter-table.html

        But not all functions are completed,
        we only wrote functions related to the current requirements,
        and other functions left function names for subsequent development

            :param talter_table: ALTER TABLE Statement to which regular expression matches
            :return: stat_dic: Key information dictionary parsed from statement
            :rtype: list
        """
        stat_dic = {}
        matchlist  = []
        stat_dic["command"] = "TALTER TABLE"
        stat_dic["name"] = talter_table.group(2).strip().strip('`')
        stat_dic["alter_cmd"] = []
        stat_dic["alter_id"] = 999
        # indexlist is the change and modify in parse_alter_table()
        indexlist = [13, 14, 25, 26]
        for index, tmatch in enumerate(self.t_alter_table_):
            matchlist.append(tmatch.match(talter_table.group(3)))
            if matchlist[index]:
                stat_dic["alter_id"] = index
                method_name = "parse_t_alter_"+str(index)
                method = getattr(self, method_name)
                if index in indexlist:
                    alter_cmd = method(talter_table.group(0))
                else:
                    alter_cmd = method(matchlist[index])
                if alter_cmd:
                    stat_dic["alter_cmd"] = alter_cmd
                    col_name = 'col_name'
                    try:
                        if stat_dic["alter_cmd"][0][col_name].upper() == "PARTITION":
                            stat_dic["alter_cmd"] = []
                            stat_dic["alter_id"] = 999
                        return stat_dic
                    except:
                        return stat_dic
        return stat_dic

    def parse_t_alter_0(self, talter_table):
        """
        parse_t_alter_0 parse MySQL DDL through regular expression list t_alter_table_[0],In the ALTER TABLE statement, match the first two MySQL alter_options, but there are two forms of column_definition
        alter_option:
            | ADD [COLUMN] col_name column_definition
                [FIRST | AFTER col_name]
            | ADD [COLUMN] (col_name column_definition,...)
        parse_t_alter_0 prase the second kind of column_column_definition
            column_definition:
                data_type
                  [COLLATE collation_name]
                  [GENERATED ALWAYS] AS (expr)
                  [VIRTUAL | STORED] [NOT NULL | NULL]
                  [VISIBLE | INVISIBLE]
                  [UNIQUE [KEY]] [[PRIMARY] KEY]
                  [COMMENT 'string']
                  [reference_definition]
                  [check_constraint_definition]
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[0].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "ADD"
            alter_dic["col_name"] = alter_item[1]
            alter_dic["data_type"] = alter_item[2]
            alter_dic["collate"] = alter_item[3].replace('collate', 'COLLATE')
            alter_dic["generated"] = alter_item[4].upper()
            alter_dic["expr"] = alter_item[5]
            alter_dic["virtual"] = alter_item[6].upper()
            alter_dic["null"] = alter_item[7].upper()
            alter_dic["visible"] = alter_item[8].upper()
            alter_dic["unique"] = alter_item[9].upper()
            alter_dic["primary"] = alter_item[10].upper()
            alter_dic["comment"] = alter_item[11].replace('comment', 'COMMENT')
            alter_dic["references"] = alter_item[12].replace('references', 'REFERENCES')
            alter_dic["references_match"] = alter_item[13].upper()
            alter_dic["references_on"] = alter_item[14].upper()
            alter_dic["check"] = alter_item[15]
            alter_dic["first"] = alter_item[16]
            alter_dic["after"] = alter_item[17]
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_1(self, talter_table):
        """
        parse_t_alter_1 parse MySQL DDL through regular expression list t_alter_table_[1],In the ALTER TABLE statement, match the first two MySQL alter_options, but there are two forms of column_definition
        alter_option:
            | ADD [COLUMN] col_name column_definition
                [FIRST | AFTER col_name]
            | ADD [COLUMN] (col_name column_definition,...)
        parse_t_alter_1 prase the first kind of column_column_definition
            column_definition:
                data_type [NOT NULL | NULL] [DEFAULT {literal | (expr)} ]
                  [VISIBLE | INVISIBLE]
                  [AUTO_INCREMENT] [UNIQUE [KEY]] [[PRIMARY] KEY]
                  [COMMENT 'string']
                  [COLLATE collation_name]
                  [COLUMN_FORMAT {FIXED | DYNAMIC | DEFAULT}]
                  [ENGINE_ATTRIBUTE [=] 'string']
                  [SECONDARY_ENGINE_ATTRIBUTE [=] 'string']
                  [STORAGE {DISK | MEMORY}]
                  [reference_definition]
                  [check_constraint_definition]
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[1].findall(alter_stat)
        for alter_item in alter_list:
            t_ig = self.t_ignore_keywords.search(alter_item[1])
            if t_ig:
                continue
            alter_dic = {}
            alter_dic["command"] = "ADD"
            alter_dic["col_name"] = alter_item[1]
            alter_dic["data_type"] = alter_item[2]
            alter_dic["null"] = alter_item[3].upper()
            alter_dic["default"] = alter_item[4].replace('default', 'DEFAULT')
            alter_dic["visible"] = alter_item[5].upper()
            alter_dic["auto_inc"] = alter_item[6].upper()
            alter_dic["unique"] = alter_item[7].upper()
            alter_dic["primary"] = alter_item[8].upper()
            alter_dic["comment"] = alter_item[9].replace('comment', 'COMMENT')
            alter_dic["collate"] = alter_item[10].replace('collate', 'COLLATE')
            alter_dic["column_format"] = alter_item[11].upper()
            alter_dic["engine"] = alter_item[12].replace('engine_attribute', 'ENGINE_ATTRIBUTE').replace(
                'secondary_engine_attribute', 'SECONDARY_ENGINE_ATTRIBUTE')
            alter_dic["storage"] = alter_item[13].upper()
            alter_dic["references"] = alter_item[14].replace('references', 'REFERENCES')
            alter_dic["references_match"] = alter_item[15].upper()
            alter_dic["references_on"] = (alter_item[16].upper()+" "+alter_item[17].upper()).strip()
            alter_dic["check"] = alter_item[18]
            alter_dic["first"] = alter_item[21]
            alter_dic["after"] = alter_item[22]
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_2(self, talter_table):
        """
        parse_t_alter_2 parse MySQL DDL through regular expression list t_alter_table_[2],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ADD {INDEX | KEY} [index_name]
                [index_type] (key_part,...) [index_option] ...
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[2].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "ADD"
            alter_dic["index_name"] = alter_item[2]
            alter_dic["index_type"] = alter_item[3].upper()
            alter_dic["key_part"] = alter_item[4]
            alter_dic["index_option_1"] = alter_item[6]
            alter_dic["index_option_2"] = alter_item[7]
            alter_dic["index_option_3"] = alter_item[8]
            alter_dic["index_option_4"] = alter_item[9]
            alter_dic["index_option_5"] = alter_item[10]
            alter_dic["col_name"] = ""
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_3(self, talter_table):
        """
        parse_t_alter_3 parse MySQL DDL through regular expression list t_alter_table_[3],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ADD {FULLTEXT | SPATIAL} [INDEX | KEY] [index_name]
                (key_part,...) [index_option] ...
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[3].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "ADD"
            alter_dic["fs"] = alter_item[1]
            alter_dic["ik"] = alter_item[2]
            alter_dic["index_name"] = alter_item[3]
            alter_dic["index_type"] = ""
            alter_dic["key_part"] = alter_item[4]
            alter_dic["index_option_1"] = alter_item[6]
            alter_dic["index_option_2"] = alter_item[7]
            alter_dic["index_option_3"] = alter_item[8]
            alter_dic["index_option_4"] = alter_item[9]
            alter_dic["index_option_5"] = alter_item[10]
            alter_dic["col_name"] = ""
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_4(self, talter_table):
        """
        parse_t_alter_4 parse MySQL DDL through regular expression list t_alter_table_[4],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ADD [CONSTRAINT [symbol]] PRIMARY KEY
                [index_type] (key_part,...) [index_option] ...
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[4].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "ADD"
            alter_dic["constraint"] = alter_item[1].replace('constraint', "CONSTRAINT")
            alter_dic["symbol"] = alter_item[2]
            alter_dic["primary"] = alter_item[3]
            alter_dic["index_type"] = alter_item[4].upper().strip()
            alter_dic["key_part"] = alter_item[5]
            alter_dic["index_option_1"] = alter_item[7]
            alter_dic["index_option_2"] = alter_item[8]
            alter_dic["index_option_3"] = alter_item[9]
            alter_dic["index_option_4"] = alter_item[10]
            alter_dic["index_option_5"] = alter_item[11]
            alter_dic["col_name"] = ""
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_5(self, talter_table):
        """
        parse_t_alter_5 parse MySQL DDL through regular expression list t_alter_table_[5],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ADD [CONSTRAINT [symbol]] UNIQUE [INDEX | KEY]
                [index_name] [index_type] (key_part,...)
                [index_option] ...
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[5].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "ADD"
            alter_dic["constraint"] = alter_item[1].replace('constraint', "CONSTRAINT")
            alter_dic["symbol"] = alter_item[2]
            alter_dic["unique"] = alter_item[3].upper().strip()
            alter_dic["key"] = alter_item[4]
            alter_dic["index_name"] = alter_item[5]
            alter_dic["index_type"] = alter_item[6]
            alter_dic["key_part"] = alter_item[7]
            alter_dic["index_option_1"] = alter_item[9]
            alter_dic["index_option_2"] = alter_item[10]
            alter_dic["index_option_3"] = alter_item[11]
            alter_dic["index_option_4"] = alter_item[12]
            alter_dic["index_option_5"] = alter_item[13]
            alter_dic["col_name"] = ""
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_6(self, talter_table):
        """
        parse_t_alter_6 parse MySQL DDL through regular expression list t_alter_table_[6],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ADD [CONSTRAINT [symbol]] FOREIGN KEY
                [index_name] (col_name,...)
                reference_definition
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[6].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "ADD"
            alter_dic["constraint"] = alter_item[1].replace('constraint', "CONSTRAINT")
            alter_dic["symbol"] = alter_item[2]
            alter_dic["foreign"] = alter_item[3].upper()
            alter_dic["index_name"] = alter_item[4]
            alter_dic["index_type"] = ""
            alter_dic["key_part"] = alter_item[5]
            alter_dic["references"] = alter_item[6].replace('references', 'REFERENCES')
            alter_dic["references_match"] = alter_item[7].upper()
            alter_dic["references_on"] = (alter_item[8].upper()+' '+alter_item[9].upper()).strip()
            alter_dic["col_name"] = ""
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_7(self, talter_table):
        """
        parse_t_alter_7 parse MySQL DDL through regular expression list t_alter_table_[7],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ADD [CONSTRAINT [symbol]] CHECK (expr) [[NOT] ENFORCED]
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_8(self, talter_table):
        """
        parse_t_alter_8 parse MySQL DDL through regular expression list t_alter_table_[8],In the ALTER TABLE statement, match the alter_option
        alter_option:
            DROP {CHECK | CONSTRAINT} symbol
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[8].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "DROP"
            alter_dic["cc"] = alter_item[1]
            alter_dic["symbol"] = alter_item[2]
            alter_dic["col_name"] = ""
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_9(self, talter_table):
        """
        parse_t_alter_9 parse MySQL DDL through regular expression list t_alter_table_[9],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ALTER {CHECK | CONSTRAINT} symbol [NOT] ENFORCED
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_10(self, talter_table):
        """
        parse_t_alter_10 parse MySQL DDL through regular expression list t_alter_table_[10],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ALGORITHM [=] {DEFAULT | INSTANT | INPLACE | COPY}
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_11(self, talter_table):
        """
        parse_t_alter_11 parse MySQL DDL through regular expression list t_alter_table_[11],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ALTER [COLUMN] col_name {
            SET DEFAULT {literal | (expr)}
            | SET {VISIBLE | INVISIBLE}
            | DROP DEFAULT
            }
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_12(self, talter_table):
        """
        parse_t_alter_12 parse MySQL DDL through regular expression list t_alter_table_[12],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ALTER INDEX index_name {VISIBLE | INVISIBLE}
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_13(self, talter_table):
        """
        parse_t_alter_13 parse MySQL DDL through regular expression list t_alter_table_[13],In the ALTER TABLE statement, match the alter_option
        alter_option:
            CHANGE [COLUMN] old_col_name new_col_name column_definition
            [FIRST | AFTER col_name]
            THE FIRST KIND column definition
        """
        malter_table = self.m_alter_table.match(talter_table)
        alter_cmd = self.parse_alter_table(malter_table)
        return alter_cmd

    def parse_t_alter_14(self, talter_table):
        """
        parse_t_alter_14 parse MySQL DDL through regular expression list t_alter_table_[14],In the ALTER TABLE statement, match the alter_option
        alter_option:
            CHANGE [COLUMN] old_col_name new_col_name column_definition
            [FIRST | AFTER col_name]
            THE second kind column definition
        """
        malter_table = self.m_alter_table.match(talter_table)
        alter_cmd = self.parse_alter_table(malter_table)
        return alter_cmd

    def parse_t_alter_15(self, talter_table):
        """
        parse_t_alter_15 parse MySQL DDL through regular expression list t_alter_table_[15],In the ALTER TABLE statement, match the alter_option
        alter_option:
            [DEFAULT] CHARACTER SET [=] charset_name [COLLATE [=] collation_name]
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_16(self, talter_table):
        """
        parse_t_alter_16 parse MySQL DDL through regular expression list t_alter_table_[16],In the ALTER TABLE statement, match the alter_option
        alter_option:
            CONVERT TO CHARACTER SET charset_name [COLLATE collation_name]
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_17(self, talter_table):
        """
        parse_t_alter_17 parse MySQL DDL through regular expression list t_alter_table_[17],In the ALTER TABLE statement, match the alter_option
        alter_option:
            {DISABLE | ENABLE} KEYS
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_18(self, talter_table):
        """
        parse_t_alter_18 parse MySQL DDL through regular expression list t_alter_table_[18],In the ALTER TABLE statement, match the alter_option
        alter_option:
            {DISCARD | IMPORT} TABLESPACE
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_19(self, talter_table):
        """
        parse_t_alter_19 parse MySQL DDL through regular expression list t_alter_table_[19],In the ALTER TABLE statement, match the alter_option
        alter_option:
            DROP {INDEX | KEY} index_name
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[19].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "DROP"
            alter_dic["col_name"] = alter_item[2]
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_20(self, talter_table):
        """
        parse_t_alter_20 parse MySQL DDL through regular expression list t_alter_table_[20],In the ALTER TABLE statement, match the alter_option
        alter_option:
            DROP PRIMARY KEY
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[20].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "DROP"
            alter_dic["col_name"] = alter_item[1]
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_21(self, talter_table):
        """
        parse_t_alter_21 parse MySQL DDL through regular expression list t_alter_table_[21],In the ALTER TABLE statement, match the alter_option
        alter_option:
            DROP FOREIGN KEY fk_symbol
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[21].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "DROP"
            alter_dic["col_name"] = alter_item[1]
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_22(self, talter_table):
        """
        parse_t_alter_22 parse MySQL DDL through regular expression list t_alter_table_[22],In the ALTER TABLE statement, match the alter_option
        alter_option:
            DROP [COLUMN] col_name
        """
        alter_cmd = []
        alter_stat = talter_table.group(0) + ','
        alter_list = self.t_alter_table_[22].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "DROP"
            alter_dic["col_name"] = alter_item[2]
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_23(self, talter_table):
        """
        parse_t_alter_23 parse MySQL DDL through regular expression list t_alter_table_[23],In the ALTER TABLE statement, match the alter_option
        alter_option:
            FORCE
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_24(self, talter_table):
        """
        parse_t_alter_24 parse MySQL DDL through regular expression list t_alter_table_[24],In the ALTER TABLE statement, match the alter_option
        alter_option:
            LOCK [=] {DEFAULT | NONE | SHARED | EXCLUSIVE}
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_25(self, talter_table):
        """
        parse_t_alter_25 parse MySQL DDL through regular expression list t_alter_table_[25],In the ALTER TABLE statement, match the alter_option
        alter_option:
            MODIFY [COLUMN] col_name column_definition
            [FIRST | AFTER col_name
            the second kind of column_definition
        """
        malter_table = self.m_alter_table.match(talter_table)
        alter_cmd = self.parse_alter_table(malter_table)
        return alter_cmd

    def parse_t_alter_26(self, talter_table):
        """
        parse_t_alter_26 parse MySQL DDL through regular expression list t_alter_table_[26],In the ALTER TABLE statement, match the alter_option
        alter_option:
            MODIFY [COLUMN] col_name column_definition
            [FIRST | AFTER col_name
            the first kind of column_definition
        """
        malter_table = self.m_alter_table.match(talter_table)
        alter_cmd = self.parse_alter_table(malter_table)
        return alter_cmd

    def parse_t_alter_27(self, talter_table):
        """
        parse_t_alter_27 parse MySQL DDL through regular expression list t_alter_table_[27],In the ALTER TABLE statement, match the alter_option
        alter_option:
            ORDER BY col_name [, col_name] ...
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_28(self, talter_table):
        """
        parse_t_alter_28 parse MySQL DDL through regular expression list t_alter_table_[28],In the ALTER TABLE statement, match the alter_option
        alter_option:
            RENAME COLUMN old_col_name TO new_col_name
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_29(self, talter_table):
        """
        parse_t_alter_29 parse MySQL DDL through regular expression list t_alter_table_[29],In the ALTER TABLE statement, match the alter_option
        alter_option:
            RENAME {INDEX | KEY} old_index_name TO new_index_name
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_t_alter_30(self, talter_table):
        """
        parse_t_alter_30 parse MySQL DDL through regular expression list t_alter_table_[30],In the ALTER TABLE statement, match the alter_option
        alter_option:
            RENAME [TO | AS] new_tbl_name
        """
        alter_cmd = []
        alter_stat = talter_table.group(0)
        alter_list = self.t_alter_table_[30].findall(alter_stat)
        for alter_item in alter_list:
            alter_dic = {}
            alter_dic["command"] = "RENAME"
            alter_dic["new_name"] = alter_item[2]
            alter_dic["col_name"] = ""
            alter_cmd.append(alter_dic)
        return alter_cmd

    def parse_t_alter_31(self, talter_table):
        """
        parse_t_alter_31 parse MySQL DDL through regular expression list t_alter_table_[31],In the ALTER TABLE statement, match the alter_option
        alter_option:
            {WITHOUT | WITH} VALIDATION
        """
        alter_cmd = []
        pass
        return alter_cmd

    def parse_rename_table(self, rename_statement):
        """
            The method parses the rename statements storing in a list the
            old and the new table name.

            :param rename_statement: The statement string without the RENAME TABLE
            :return: rename_list, a list with the old/new table names inside
            :rtype: list

        """
        rename_list = []
        for rename in rename_statement.split(','):
            mrename_items = self.m_rename_items.search(rename.strip())
            if mrename_items:
                rename_list.append([item.strip().replace('`', '') for item in mrename_items.groups()])
        return rename_list

    def parse_sql(self, sql_string):
        """
            The method cleans and parses the sql string
            A regular expression replaces all the default value definitions with a space.
            Then the statements are split in a list using the statement separator;

            For each statement a set of regular expressions remove the comments, single and multi line.
            Parenthesis are surrounded by spaces and commas are rewritten in order to get at least one space after the comma.
            The statement is then put on a single line and stripped.

            Different match are performed on the statement.
            RENAME TABLE
            CREATE TABLE
            DROP TABLE
            ALTER TABLE
            ALTER INDEX
            DROP PRIMARY KEY
            TRUNCATE TABLE

            CREATE INDEX
            DROP INDEX
            (T)ALTER TABLE the T is written to distinguish between different developer versions

            The match which is successful determines the parsing of the rest of the statement.
            Each parse builds a dictionary with at least two keys "name" and "command".

            Each statement parse comes with specific addictional keys.

            When the token dictionary is complete is added to the class list tokenised

            :param sql_string: The sql string with the sql statements.
        """
        statements=sql_string.split(';')
        for statement in statements:
            stat_dic={}
            stat_cleanup=re.sub(r'/\*.*?\*/', '', statement, re.DOTALL)
            stat_cleanup=re.sub(r'--.*?\n', '', stat_cleanup)
            stat_cleanup=re.sub(r'[\b)\b]', ' ) ', stat_cleanup)
            stat_cleanup=re.sub(r'[\b(\b]', ' ( ', stat_cleanup)
            stat_cleanup=re.sub(r'[\b,\b]', ', ', stat_cleanup)
            stat_cleanup=stat_cleanup.replace('\n', ' ')
            stat_cleanup = re.sub("\([\w*\s*]\)", " ",  stat_cleanup)
            stat_cleanup = stat_cleanup.strip()
            malter_rename = self.m_alter_rename_table.match(stat_cleanup)
            mrename_table = self.m_rename_table.match(stat_cleanup)
            mcreate_table = self.m_create_table.match(stat_cleanup)
            mdrop_table = self.m_drop_table.match(stat_cleanup)
            malter_table = self.m_alter_table.match(stat_cleanup)
            mdrop_primary = self.m_drop_primary.match(stat_cleanup)
            mtruncate_table = self.m_truncate_table.match(stat_cleanup)
            mcreate_full_index = self.m_create_full_index.match(stat_cleanup)
            mdrop_full_index = self.m_drop_full_index.match(stat_cleanup)
            talter_table = self.t_alter_table.match(stat_cleanup)
            if talter_table:
                stat_dic = self.parse_t_alter_table(talter_table)
                try:
                    stat_dic["alt_partition"] = self.parse_alter_partition(stat_cleanup)
                except:
                    stat_dic = {}
                if len(stat_dic["alter_cmd"]) == 0 and len(stat_dic["alt_partition"]) == 0:
                    stat_dic = {}
            elif malter_rename:
                stat_dic["command"] = "RENAME TABLE"
                stat_dic["name"] = malter_rename.group(2)
                stat_dic["new_name"] = malter_rename.group(3)
                self.tokenised.append(stat_dic)
                stat_dic = {}
            elif mrename_table:
                rename_list = self.parse_rename_table(mrename_table.group(2))
                for rename_table in rename_list:
                    stat_dic["command"] = "RENAME TABLE"
                    stat_dic["name"] = rename_table[0]
                    stat_dic["new_name"] = rename_table[1]
                    self.tokenised.append(stat_dic)
                    stat_dic = {}
            elif mcreate_full_index:
                command = "CREATE INDEX FULL"
                stat_dic["command"] = command
                stat_dic["UFS"] = mcreate_full_index.group(2)
                if stat_dic["UFS"]:
                    stat_dic["UFS"]= stat_dic["UFS"].upper()
                stat_dic["index_name"] = mcreate_full_index.group(4)
                stat_dic["index_type"] = mcreate_full_index.group(5)
                stat_dic["name"] = mcreate_full_index.group(6)
                stat_dic["key_part"] = ''.join((mcreate_full_index.group(7), mcreate_full_index.group(8)))
                stat_dic["index_option"] = mcreate_full_index.group(9)
                stat_dic["algorithm_lock_option"] = mcreate_full_index.group(10)
            elif mdrop_full_index:
                command = "DROP INDEX FULL"
                stat_dic["command"] = command
                stat_dic["index_name"] = mdrop_full_index.group(2)
                stat_dic["name"] = mdrop_full_index.group(3)
                stat_dic["algorithm_lock_option"] = mdrop_full_index.group(4)
            elif mcreate_table:
                command=' '.join(mcreate_table.group(1).split()).upper().strip()
                stat_dic["command"]=command
                stat_dic["name"] = mcreate_table.group(2)
                gp2 = stat_cleanup.replace("partition", "PARTITION")
                par = gp2.find("PARTITION")
                parti = ""
                if (par == -1):
                    stat_cleanup = gp2
                else:
                    stat_cleanup = gp2[:par]
                    parti = gp2[par:]
                create_parsed=self.parse_create_table(stat_cleanup, stat_dic["name"])
                stat_dic["columns"] = create_parsed["columns"]
                stat_dic["indices"] = create_parsed["indices"]
                stat_dic["comment"] = create_parsed["comment"]
                partition = self.parse_partition(parti)
                stat_dic["partition"] = partition
            elif mdrop_table:
                command=' '.join(mdrop_table.group(1).split()).upper().strip()
                stat_dic["command"]=command
                stat_dic["name"]=mdrop_table.group(2)
            elif mtruncate_table:
                command=' '.join(mtruncate_table.group(1).split()).upper().strip()
                stat_dic["command"]=command
                if mtruncate_table.group(3) == '':
                    stat_dic["name"]=mtruncate_table.group(2)
                else:
                    stat_dic["name"]=mtruncate_table.group(3)
            elif mdrop_primary:
                stat_dic["command"]="DROP PRIMARY KEY"
                stat_dic["name"]=mdrop_primary.group(1).strip().strip(',').replace('`', '').strip()
            elif malter_table:
                stat_dic = self.parse_alter_table(malter_table)
                if len(stat_dic["alter_cmd"]) == 0:
                    stat_dic = {}

            if stat_dic!={}:
                self.tokenised.append(stat_dic)

    def parse_version(version_str):
        suffix_pos = version_str.find('-')
        if (suffix_pos != -1):
            num_str = version_str[0:suffix_pos]
        else:
            num_str = version_str
        split_num = num_str.split('.')
        version_num = 0
        for v in split_num:
            version_num = version_num * sql_token.VERSION_SCALE + int(v)
        return version_num


@unique
class DBObjectType(Enum):
    """
    Enumeration class for database object types.
    """
    VIEW = "view"
    TRIGGER = "trigger"
    PROC = "procedure"
    FUNC = "function"

    def sql_to_get_object_metadata(self):
        """
        This method can obtain the sql which can get the object metadata.
        The obtained sql is in mysql dialect format.

        :return: target sql string
        """
        sql_on_dict = {
            DBObjectType.VIEW: "SELECT TABLE_NAME AS OBJECT_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '%s';",
            DBObjectType.TRIGGER: "SELECT TRIGGER_NAME AS OBJECT_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = '%s';",
            DBObjectType.PROC: "SELECT NAME AS OBJECT_NAME FROM mysql.proc WHERE type = 'PROCEDURE' AND db = '%s';",
            DBObjectType.FUNC: "SELECT NAME AS OBJECT_NAME FROM mysql.proc WHERE type = 'FUNCTION' AND db = '%s';"
        }
        return sql_on_dict[self]

    def sql_to_get_create_object_statement(self):
        """
        This method can obtain the sql, which use to get source database details about creating object.
        The obtained sql is in mysql dialect format.

        :return: target sql string
        """
        return "SHOW CREATE {} `%s`.`%s`;".format(self.value.upper())


class SqlTranslator():

    def __init__(self):
        """
        Class constructor.
        This method sets the value of lib_dir to the absolute path of the lib directory.

        """
        self.lib_dir = os.path.dirname(os.path.realpath(__file__))

    def mysql_to_opengauss(self, raw_sql):
        """
        Translate sql dialect in mysql format to opengauss format.
        Implement the power of translating sql statements with calling java subproject og-translator through CMD.

        :param raw_sql: the sql in mysql dialect format
        :return: a tuple with stdout, stderr_in_list
        """
        # use base64 to encode raw sql, avoiding the impact of special symbols on bash
        sql_encoded = str(base64.b64encode(raw_sql.encode("utf-8")), "utf-8")
        # chameleon calling the java subproject og-translator to implement the power of translating sql statements
        cmd = "java -jar %s/sql-translator-1.0.jar --base64 '%s'" % (self.lib_dir, sql_encoded)
        communicate = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

        stdout = communicate[0].decode("utf-8").replace("`", "")  # sql translated into opengauss dialect format
        stderr = communicate[1].decode("utf-8")  # logs generated during translation
        return stdout, stderr
