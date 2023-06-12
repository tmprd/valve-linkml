import csv

def write_dicts2tsv(filepath: str, rendered_data: list, headers: list) -> str:
    with open(filepath, "w") as file:
        writer = csv.DictWriter(file, delimiter="\t", fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rendered_data)
    print(f"Wrote to '{filepath}'")