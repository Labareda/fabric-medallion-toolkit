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
