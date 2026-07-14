"""
Generic dependency-graph ordering -- not Gold-specific, not Jira-specific.
Given a set of steps and which other steps each one depends on, returns a
valid run order (every dependency before whatever needs it), or raises a
clear error if the dependencies contain a cycle (a genuine configuration
bug worth catching immediately, rather than an orchestration notebook
silently running things in a broken order or hanging).

Kahn's algorithm: repeatedly take any step with no remaining unsatisfied
dependencies, "run" it (remove it from the graph), and repeat. If steps
remain but none has zero remaining dependencies, whatever's left forms a
cycle.
"""

from typing import Dict, List


def topological_sort(dependencies: Dict[str, List[str]]) -> List[str]:
    """
    dependencies: {step_name: [names of steps it depends on]}. A step with
        no dependencies should still appear as a key, with an empty list --
        e.g. {"Dim_Project": [], "Fact_Issue": ["Dim_Project", "Dim_User"]}.
        Referencing a dependency name that never appears as its own key is
        an error (can't depend on a step that isn't defined anywhere).

    Returns: step names in an order where every dependency comes before
        anything that depends on it. Steps with no dependency relationship
        to each other come back in a stable, deterministic order (sorted
        alphabetically among whatever's currently runnable) rather than
        arbitrary, so re-running with the same input always produces the
        same order.

    Raises ValueError on an unknown dependency reference, or a cycle.
    """
    all_steps = set(dependencies.keys())
    for step, deps in dependencies.items():
        for dep in deps:
            if dep not in all_steps:
                raise ValueError(
                    f"'{step}' depends on '{dep}', but '{dep}' is not itself a key in "
                    f"dependencies -- every dependency must also be a defined step."
                )

    remaining = {step: set(deps) for step, deps in dependencies.items()}
    ordered: List[str] = []

    while remaining:
        # Any step whose dependencies are all already satisfied (ordered)
        ready = sorted(step for step, deps in remaining.items() if not deps)
        if not ready:
            raise ValueError(
                f"Circular dependency detected among: {sorted(remaining.keys())} -- "
                f"none of these have all their dependencies satisfied, so no valid "
                f"run order exists. Check for a step that (directly or indirectly) "
                f"depends on itself."
            )
        for step in ready:
            ordered.append(step)
            del remaining[step]
        for deps in remaining.values():
            deps.difference_update(ready)

    return ordered


def build_medallion_run_order(
    source_to_bronze: List[str],
    bronze_to_silver: List[str],
    dimensions,
    facts: Dict[str, List[str]],
) -> List[str]:
    """
    Builds the standard medallion dependency shape and returns the
    topologically-sorted run order in one call, instead of hand-building
    the merged dependency dict yourself every time:

    - Every bronze step has no dependencies (the start of the pipeline).
    - Every silver step depends on ALL bronze steps.
    - Every dimension depends on ALL silver steps, PLUS whatever OTHER
      dimensions it declares (see `dimensions` below) -- e.g. a dimension
      that derives a column by joining to another dimension (grouping a
      hierarchy's roots by project code, say) genuinely needs that other
      dimension to already exist.
    - Every fact depends on ALL dimensions, PLUS whatever OTHER facts it
      declares in its own entry in `facts` (empty list if none).

    This is the "barrier" pattern most medallion pipelines share --
    generic regardless of which actual notebooks you're running, unlike
    the notebook names themselves (source_to_bronze, bronze_to_silver,
    dimensions, facts), which stay specific to your own pipeline and are
    passed in here as plain data.

    dimensions: EITHER
        - a plain list of dimension names, when none of them depend on
          each other (the common case) -- e.g. ["Dim_Date", "Dim_Project"]
        - a dict {dimension_name: [other_dimension_names_it_depends_on]}
          when at least one does -- e.g.
          {"Dim_Project": [], "Dim_Issue": ["Dim_Project"]}
          every dimension must still appear as a key even with an empty
          list, same rule as topological_sort itself.

    Without an explicit dependency, two dimensions with no relationship
    to each other are ordered alphabetically (topological_sort's own
    tie-breaking) -- NOT by which one happens to need the other. Declare
    the dependency explicitly rather than relying on alphabetical order
    coincidentally working out.

    facts: {fact_notebook_name: [other_fact_notebook_names_it_depends_on]}
    """
    dimension_deps: Dict[str, List[str]] = (
        dict(dimensions) if isinstance(dimensions, dict) else {d: [] for d in dimensions}
    )
    dimension_names = list(dimension_deps.keys())

    full_dependencies: Dict[str, List[str]] = {}

    for step in source_to_bronze:
        full_dependencies[step] = []
    for step in bronze_to_silver:
        full_dependencies[step] = list(source_to_bronze)
    for dim, dim_deps in dimension_deps.items():
        full_dependencies[dim] = list(bronze_to_silver) + list(dim_deps)
    for fact, fact_deps in facts.items():
        full_dependencies[fact] = dimension_names + fact_deps

    return topological_sort(full_dependencies)
