"""The plan IR: typed plan nodes (`node`), expressions (`expr`), scope and
column types (`types`), the reference evaluator (`eval`) and the result
metadata it carries. `match` lowers its argument to a plan of these nodes;
the engine executes the plan, and the reference evaluator executes the
same plan where the engine does not. Import the submodules directly.
"""
