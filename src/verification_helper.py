# Check this doc for definition around the constraints:
# https://pydeequ.readthedocs.io/en/latest/pydeequ.html


def construct_single_column_constraint_check(constraints_name, columns):
    check_string = ""
    for column in columns:
        check_string += ".{}('{}')".format(constraints_name, column)
    return check_string


def construct_dual_column_constraint_check(constraints_name, columns):
    check_string = ""
    for rule in columns:
        check_string += ".{}('{}', '{}')".format(constraints_name, rule[0], rule[1])
    return check_string


def construct_multi_column_constraint_check(constraints_name, columns):
    check_string = ""
    for column_list in columns:
        check_string += ".{}({})".format(constraints_name, column_list)
    return check_string


def construct_assertion_based_single_column_constraint_check(constraints_name, columns):
    check_string = ""
    for rule in columns:
        check_string += ".{}('{}', lambda x: x >= {}, 'It should be above {}')".format(
            constraints_name, rule[0], rule[1], rule[1]
        )
    return check_string


def construct_assertion_based_dual_column_constraint_check(constraints_name, columns):
    check_string = ""
    for rule in columns:
        check_string += ".{}('{}', '{}', lambda x: x >= {})".format(
            constraints_name, rule[0], rule[1], rule[2]
        )
    return check_string


def construct_multi_column_assertion_and_condition_based_constraint_check(
    constraints_name, columns
):
    check_string = ""
    for rule in columns:
        check_string += ".{}('{}', '{}', lambda x: x == 1.0)".format(
            constraints_name, rule, rule
        )
    return check_string


CONSTRUCT_CHECKS_RULE_ENGINE = {
    "isComplete": construct_single_column_constraint_check,
    "isUnique": construct_single_column_constraint_check,
    "isNonNegative": construct_single_column_constraint_check,
    "containsURL": construct_single_column_constraint_check,
    "containsEmail": construct_single_column_constraint_check,
    "isPositive": construct_single_column_constraint_check,
    "containsCreditCardNumber": construct_single_column_constraint_check,
    "areComplete": construct_multi_column_constraint_check,
    "isGreaterThan": construct_dual_column_constraint_check,
    "isGreaterThanOrEqualTo": construct_dual_column_constraint_check,
    "isLessThan": construct_dual_column_constraint_check,
    "isLessThanOrEqualTo": construct_dual_column_constraint_check,
    "hasCompleteness": construct_assertion_based_single_column_constraint_check,
    "hasDataType": construct_assertion_based_single_column_constraint_check,
    "hasDistinctness": construct_assertion_based_single_column_constraint_check,
    "isContainedIn": construct_assertion_based_single_column_constraint_check,
    "hasEntropy": construct_assertion_based_single_column_constraint_check,
    "hasMax": construct_assertion_based_single_column_constraint_check,
    "hasMaxLength": construct_assertion_based_single_column_constraint_check,
    "hasMean": construct_assertion_based_single_column_constraint_check,
    "hasMin": construct_assertion_based_single_column_constraint_check,
    "hasMinLength": construct_assertion_based_single_column_constraint_check,
    "hasPattern": construct_assertion_based_single_column_constraint_check,
    "hasStandardDeviation": construct_assertion_based_single_column_constraint_check,
    "hasSum": construct_assertion_based_single_column_constraint_check,
    "hasUniqueness": construct_assertion_based_single_column_constraint_check,
    "hasCorrelation": construct_assertion_based_dual_column_constraint_check,
    "hasMutualInformation": construct_assertion_based_dual_column_constraint_check,
    "satisfies": construct_multi_column_assertion_and_condition_based_constraint_check,
}


def validate_data_verification_job_config(config):
    all_constraints = CONSTRUCT_CHECKS_RULE_ENGINE.keys()

    for key, val in config.items():
        if key not in all_constraints:
            raise KeyError("{} not in constrains_list: {}".format(key, all_constraints))


def construct_data_verification_checks(constraints_configs):
    validate_data_verification_job_config(constraints_configs)

    check_string = "check"
    for constraints_name, columns in constraints_configs.items():
        check_string += CONSTRUCT_CHECKS_RULE_ENGINE.get(constraints_name)(
            constraints_name, columns
        )

    return check_string


# The commented code is handy in testing the correctness of check_string once any new constraint
# is added or malfunctioning in existing check_string

"""
# if __name__ == '__main__':
#     import simplejson
#     from collections import OrderedDict
# 
#     with open("config_test.json") as config_file:
#         config = simplejson.load(config_file, object_pairs_hook=OrderedDict)
#     print(construct_data_verification_checks(config["constraints"]))
"""
