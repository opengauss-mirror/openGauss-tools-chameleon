import json
import re
import logging
from kafka import KafkaProducer
from pg_chameleon.lib.error_code import ErrorCode

class KafkaHandler(logging.Handler):
    def __init__(self, alert_log_kafka_server, topic, log_filter_pattern=None, sent_error_log_count=0):
        logging.Handler.__init__(self)
        self.producer = KafkaProducer(
            bootstrap_servers=alert_log_kafka_server,
            value_serializer=lambda v: str(v).encode('utf-8')
        )
        self.alert_log_kafka_server = alert_log_kafka_server
        self.topic = topic
        self.log_filter_pattern = log_filter_pattern
        self.sent_error_log_count = sent_error_log_count

    def register_error_code(self):
        # Build a map of error codes
        code_cause_cn_map = {}
        code_cause_en_map = {}

        for error_code in ErrorCode:
            code_cause_cn_map[error_code.code] = error_code.cause_cn
            code_cause_en_map[error_code.code] = error_code.cause_en

        # Serialize the error code information into a JSON string
        code_cause_cn_string = json.dumps(code_cause_cn_map, ensure_ascii=False)
        code_cause_en_string = json.dumps(code_cause_en_map, ensure_ascii=False)

        producer.send(self.topic, key=b"chameleon", value=f"<CODE:causeCn>{code_cause_cn_string}<CODE:causeEn>{code_cause_en_string}")

    def emit(self, record):
        try:
            if self.should_log(record):
                log_entry = self.format(record)
                self.producer.send(self.topic, key=b"chameleon", value=log_entry)
                self.producer.flush()
                self.producer.close()
        except Exception:
            self.handleError(record)

    def should_log(self, record):
        if self.log_filter_pattern:
            if re.search(self.log_filter_pattern, record.getMessage()):
                if self.sent_error_log_count == 0:
                    self.register_error_code()
                self.sent_error_log_count += 1
                return True
            else:
                return False
        return True

    def get_error_log_count(self):
        return self.sent_error_log_count

    def close(self):
        # Shut down KafkaProducer
        self.producer.close()
        super().close()
