"""Sets the protein field to the default based on boxt type."""

import json
from pathlib import Path
from typing import Any

FIXTURES_FP = Path(__file__).parent.parent / "tests" / "unit" / "fixtures"
PROTEIN_BOX_TYPES = ["BASIC", "GF", "LA"]


def write_protein_field_to_stops_responses(fp: Path) -> None:
    """Writes the protein field to the stops responses data."""
    with open(fp, "r") as file:
        data = json.load(file)

    data_with_protein = add_protein_field(data)

    output_fp = fp.parent / f"{fp.stem}_with_protein{fp.suffix}"
    with open(output_fp, "w") as file:
        json.dump(data_with_protein, file, indent=4)


def add_protein_field(data):  # noqa: ANN001, ANN201
    """Adds the protein field to the stops responses data."""
    new_data = data.copy()

    for i in range(len(data)):
        stops_array: list[dict[str, Any]] = data[i]
        for j in range(len(stops_array)):
            stops = stops_array[j].get("stops")
            for k in range(len(stops)):
                stop = stops[k]
                order_info = stop.get("orderInfo")
                products = order_info.get("products")
                if products is not None and len(products) > 0:
                    box_type = order_info.get("products")[0]
                else:
                    # The only stop without a product should be the depot stop.
                    stop_position = stop.get("stopPosition")
                    route = stop.get("route")
                    stop_count = route.get("stopCount")
                    if stop_position != 0 and stop_position != stop_count + 1:
                        breakpoint()
                    else:
                        box_type = "depot"
                if box_type in PROTEIN_BOX_TYPES:
                    protein_value = True
                elif box_type == "depot":
                    # NOTE: This is not a nullable field in our schema,
                    # but I want to find where the depot stops get added.
                    protein_value = None
                else:
                    protein_value = False

                new_data[i][j]["stops"][k]["customProperties"] = {"protein": protein_value}

    return new_data


if __name__ == "__main__":
    for file_name in ["stops_responses_all_hhs.json", "stops_responses.json"]:
        write_protein_field_to_stops_responses(FIXTURES_FP / file_name)
