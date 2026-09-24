def double_tasks(tasks: list[dict]) -> list[int]:
    """Double the values in each task payload."""
    return [task["value"] * 2 for task in tasks]
