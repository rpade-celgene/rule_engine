import abc
from lib.ETLUtilities.Logger import logger


class IRule(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, data, config):
        self.data = data
        self.config = config
        logger.info("Validating " + self.rule_name + " rule")
        self.validation_result = self.validate()

    @property
    @abc.abstractmethod
    def rule_name(self):
        return "The name of the rule or event type"

    @abc.abstractmethod
    def validate(self):
        pass

    @abc.abstractmethod
    def success_message(self, record_name):
        return "Everything is fine for record " + record_name

    @abc.abstractmethod
    def fail_message(self, record_name):
        return "There is definitely something wrong with " + record_name

    def result_message(self, record_result):
        if record_result:
            return self.success_message()
        else:
            return self.fail_message()

    def result_to_event(self, dag_name, dag_run_id):
        # replace True/False with result_message
        # change dataframe to json
        event = {
            'dag_name': dag_name,
            'dag_run_id': dag_run_id,
            'event_type': self.rule_name(),
            'event_data': self.result_data()
        }
        return event
