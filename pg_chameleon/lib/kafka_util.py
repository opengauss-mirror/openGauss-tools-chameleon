import json
import re
import logging
from kafka import KafkaProducer
from pg_chameleon.lib.error_code import ErrorCode
from concurrent.futures import ThreadPoolExecutor
import threading

class KafkaHandler(logging.Handler):
    def __init__(self, alert_log_kafka_server, topic, log_filter_pattern=None, sent_error_log_count=0):
        logging.Handler.__init__(self)
        self.alert_log_kafka_server = alert_log_kafka_server
        self.topic = topic
        self.log_filter_pattern = log_filter_pattern
        self.sent_error_log_count = sent_error_log_count
        self.producer = None
        self.executor = ThreadPoolExecutor(max_workers=10)

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

        # Create KafkaProducer and send error codes
        producer = KafkaProducer(
            bootstrap_servers=self.alert_log_kafka_server,
            key_serializer=str.encode,
            value_serializer=lambda v: str(v).encode('utf-8')
        )
        producer.send(self.topic, key="chameleon", value=f"<CODE:causeCn>{code_cause_cn_string}<CODE:causeEn>{code_cause_en_string}")
        producer.flush(timeout=10)
        producer.close()

    def get_producer(self):
        """ Initialize KafkaProducer once and reuse it """
        if not self.producer:
            self.producer = KafkaProducer(
                bootstrap_servers=self.alert_log_kafka_server,
                value_serializer=lambda v: str(v).encode('utf-8')
            )
        return self.producer

    def send_log_to_kafka(self, log_entry):
        """ Asynchronous log sending method """
        try:
            producer = self.get_producer()
            producer.send(self.topic, key=b"chameleon", value=log_entry)
            producer.flush(timeout=10)  # Set a timeout to avoid blocking indefinitely
        except Exception as e:
            logging.error(f"Error sending log to Kafka: {e}")

    def emit(self, record):
        """ Override emit method to log asynchronously """
        try:
            if self.should_log(record):
                log_entry = self.format(record)
                # Submit task to the thread pool for async processing
                self.executor.submit(self.send_log_to_kafka, log_entry)
        except Exception:
            self.handleError(record)

    def should_log(self, record):
        """ Determine if the log should be sent """
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
        """ Get the count of sent error logs """
        return self.sent_error_log_count

    def close(self):
        """ Override close to clean up resources """
        if self.producer:
            self.producer.close()
        self.executor.shutdown(wait=True)
        super().close()