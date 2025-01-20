from enum import Enum

class ErrorCode(Enum):
    UNKNOWN = (5000, "未知异常", "Unknown error")
    INDEX_FILE_MISSING_DATA = (5010, "索引文件中缺少数据", "The index file is missing data")
    UNSUPPORTED_TYPE_CONVERSION = (5020, "不支持的类型转换", "Unsupported type conversion")
    INCORRECT_CONFIGURATION = (5030, "参数配置错误", "Incorrect configuration error")
    SOURCE_NOT_FOUND_IN_CONFIGURATION = (5031, "配置中的源缺失", "The source in the configuration is missing")
    INVALID_LOG_LEVEL = (5040, "无效的日志级别值", "Invalid log level specified")
    CSV_FILE_MOVE_FAILED = (5050, "csv文件移动失败", "Csv file move failed")
    CSV_FILE_REMOVE_FAILED = (5051, "csv文件删除失败", "Csv file remove failed")
    META_GENERATION_FAILED = (5060, "元数据生成失败", "Metadata generation failed.")
    NETWORK_OR_SERVICE_UNAVAILABLE = (5100, "网络或服务异常", "Network or Service Exception")
    PROCESS_STATE_CHECK_EXCEPTION = (5110, "进程状态检查失败", "Process state check failed")
    PROCESS_TERMINATE_EXCEPTION = (5120, "进程终止失败", "Process terminate failed")
    QUEUE_READ_EXCEPTION = (5130, "队列读取失败", "Queue read failed")
    OPENGAUSS_DB_CONNECTION_FAILED = (5300, "OpenGauss数据库连接失败", "OpenGauss Database connection failed")
    OPENGAUSS_DB_OPERATION_FAILED = (5301, "OpenGauss数据库操作失败", "OpenGauss Database operation failed")
    OPENGAUSS_DB_SET_STATUS_FAILED = (5302, "设置OpenGauss数据库状态失败",
        "Failed to set OpenGauss database status")
    OPENGAUSS_DB_STATUS_ABNORMAL = (5303, "OpenGauss数据库状态异常", "OpenGauss Database status abnormal")
    OPENGAUSS_DB_STATUS_BACK_NORMAL = (5304, "OpenGauss数据库状态已恢复正常",
        "OpenGauss Database status is back to normal")
    MYSQL_DB_CONNECTION_FAILED = (5310, "Mysql数据库连接失败", "MYSQL Database connection failed")
    SQL_EXCEPTION = (5320, "SQL执行失败", "SQL execution failed")
    OBJECT_MIGRATION_USER_MISSING = (5321, "对象迁移用户缺失", "Migration user for object is missing")
    OBJECT_MIGRATION_FAILED = (5322, "对象迁移失败", "Object migration failed")
    GET_SOURCE_SCHEMA_CHARSET_COLLATE_FAILED = (
    5323, "获取源架构字符集和排序规则失败", "Failed to get source schema character set and collate")
    GET_SOURCE_DATA_FAILED = (5324, "获取源数据失败", "Failed to get source data")
    CREATE_INDEX_FAILED = (5325, "创建索引失败", "Failed to create index for the specified schema.table")
    PARALLEL_INDEX_RESET_FAILED = (5326, "并行索引重置失败", "Parallel index reset failed")
    AUTO_INCREMENT_INDEX_CREATE_FAILED = (5327, "自增索引创建失败", "Auto increment index creation failed")
    SQL_VACUUM_EXECUTION_FAILED = (5328, "VACUUM 执行失败", "VACUUM execution failed")
    SQL_COPY_CSV_MODE_FAILED = (5329, "CSV 文件导入模式失败", "CSV copy mode execution failed")

    def __init__(self, code, cause_cn, cause_en):
        self._code = code
        self._cause_cn = cause_cn
        self._cause_en = cause_en

    @property
    def code(self):
        return self._code

    @property
    def cause_cn(self):
        return self._cause_cn

    @property
    def cause_en(self):
        return self._cause_en

    def get_error_prefix(self):
        return f"<CODE:{self.code}>"

    def __str__(self):
        return self.get_error_prefix()