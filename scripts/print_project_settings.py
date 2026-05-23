"""Print MerchantLift BI project settings.

This script proves that our config loader and path utilities work.
"""

from merchantlift.config import load_yaml_config
from merchantlift.paths import CONFIG_DIR


def main() -> None:
    """Load and print core project settings."""
    config_path = CONFIG_DIR / "project_settings.yaml"
    settings = load_yaml_config(config_path)

    print(settings)

    project_name = settings["project"]["name"]
    minimum_cohort_size = settings["privacy"]["minimum_reportable_cohort_size"]
    raw_zone = settings["lakehouse"]["raw_zone"]

    print(f"Project name: {project_name}")
    print(f"Minimum reportable cohort size: {minimum_cohort_size}")
    print(f"Raw data zone: {raw_zone}")


if __name__ == "__main__":
    main()