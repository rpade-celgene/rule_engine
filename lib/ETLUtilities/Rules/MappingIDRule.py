from lib.ETLUtilities.IRule import IRule


class MappingIDRule(IRule):
    def __init__(self, data, config):
        super(MappingIDRule, self).__init__(data, config)

    @property
    def rule_name(self):
        return "Mapping ID"

    def validate(self):
        result_column = self.data['subset'][self.config].isin(self.data['superset'][self.config])
        result_set = self.data['subset'].copy()
        result_set['result'] = result_column.values
        return result_set

    def success_message(self, record_name):
        return "Success: " + record_name + "has mappable primary keys"

    def fail_message(self, record_name):
        return "Fail: " + record_name + " does not have mappable primary keys"


