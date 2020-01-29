from lib.ETLUtilities.IRule import IRule
import pandas as pd


class UniquePKRule(IRule):
    def __init__(self, data, config):
        super(UniquePKRule, self).__init__(data, config)

    @property
    def rule_name(self):
        return "Unique PK"

    def validate(self):
        df_count = self.data.groupby(self.config).size().reset_index(name='counts')
        df_dup = df_count[df_count.counts >= 2].sort_values(self.config)

        index1 = pd.MultiIndex.from_arrays([df_dup[col] for col in self.config])
        index2 = pd.MultiIndex.from_arrays([self.data[col] for col in self.config])
        result_column = ~index2.isin(index1)

        result_set = self.data.copy()
        result_set['result'] = result_column

        return result_set

    def success_message(self, record_name):
        return "Success: " + record_name + " has a unique PK"

    def fail_message(self, record_name):
        return "Fail: " + record_name + " does not have a unique PK"



