import csv
import math
import random

def write_dicts2tsv(filepath: str, rendered_data: list, headers: list) -> None:
    with open(filepath, "w") as file:
        writer = csv.DictWriter(file, delimiter="\t", fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        if rendered_data is not None:
            writer.writerows(rendered_data)


def generate_error_set(count: int, error_rate: float) -> list:
    error_count = math.floor(count * error_rate)
    error_rows = []
    for i in range(1, error_count):
        error_rows.append(random.randint(1, count))
    return sorted(set(error_rows))