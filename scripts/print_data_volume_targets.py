"""Print MerchantLift BI data volume and synthetic generation settings."""

from merchantlift.config import load_yaml_config
from merchantlift.paths import CONFIG_DIR


def main() -> None:
    """Load and print data volume and generation settings."""
    settings = load_yaml_config(CONFIG_DIR / "project_settings.yaml")

    targets = settings["data_volume_targets"]
    sample_scale = settings["synthetic_generation"]["local_sample_scale"]
    behavior_mix = settings["synthetic_generation"]["behavior_mix"]

    total_target_rows = sum(targets.values())
    total_sample_rows = sum(sample_scale.values())

    print("MerchantLift BI Data Volume Targets")
    print("-----------------------------------")
    print(f"Full target rows: {total_target_rows:,}")
    print(f"Local sample rows: {total_sample_rows:,}")

    print("\nFull table targets:")
    for table_name, row_count in targets.items():
        print(f"  {table_name}: {row_count:,}")

    print("\nSynthetic behavior mix:")
    for behavior_type, share in behavior_mix.items():
        print(f"  {behavior_type}: {share:.0%}")


if __name__ == "__main__":
    main()