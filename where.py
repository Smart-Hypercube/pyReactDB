from pyparsing import *

# https://ronsavage.github.io/SQL/sql-2003-2.bnf.html

AND = CaselessLiteral('and')
IS = CaselessLiteral('is')
NOT = CaselessLiteral('not')
OR = CaselessLiteral('or')

predicate = (comparision_predicate
             | between_predicate
             | in_predicate
             # | like_predicate
             # | similar_predicate
             | null_predicate
             # | quantified_comparison_predicate
             # | exists_predicate
             # | unique_predicate
             # | normalized_predicate
             # | match_predicate
             # | overlaps_predicate
             # | distinct_predicate
             # | member_predicate
             # | submultiset_predicate
             # | set_predicate
             # | type_predicate
             )
parenthesized_boolean_value_expression = left_paren + boolean_value_expression
boolean_predicand = parenthesized_boolean_value_expression | nonparenthesized_value_expression_primary
boolean_primary = predicate | boolean_predicand
truth_value =
boolean_test = boolean_primary + Optional(IS + Optional(NOT) + truth_value)
boolean_factor = Optional(NOT + boolean_test)
boolean_term = boolean_factor + ZeroOrMore(AND + boolean_factor)
boolean_value_expression = boolean_term + ZeroOrMore(OR + boolean_term)
search_condition = boolean_value_expression
where = Optional(search_condition, '')

if __name__ == '__main__':
    print(repr(where.))
