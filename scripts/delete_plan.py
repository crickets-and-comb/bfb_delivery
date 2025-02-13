"""Delete a plan from Circuit."""

import logging

import click

from bfb_delivery.lib.dispatch.write_to_circuit import delete_plans

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--plan_id", type=str, required=False, help="The plan ID to be deleted. As 'plans/{id}'."
)
@click.option(
    "--plan_df_fp",
    type=str,
    required=False,
    # default=".test_data/scratch/plans/plans.csv",
    help="The file path to a dataframe with plan IDs to be deleted in column 'plan_id'.",
)
def main(plan_id: str, plan_df_fp: str) -> list[str]:
    """Delete a plan from Circuit."""
    if plan_id and plan_df_fp:
        raise ValueError("Please provide either a plan_id or a file path, not both.")
    if not plan_id and not plan_df_fp:
        raise ValueError("Please provide either a plan_id or a plan_df_fp.")

    plan_ids = []
    if plan_id:
        plan_ids = [plan_id]

    plans = delete_plans(plan_ids=plan_ids, plan_df_fp=plan_df_fp)

    print(f"Deleted:\n{plans}.")

    return plans


if __name__ == "__main__":
    main()
