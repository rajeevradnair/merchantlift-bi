"""
Practice script for understanding incrementality.

This script compares two approaches:
1. Naive before/after measurement
2. Stronger test/control measurement

The goal is to see why before/after analysis can be misleading.
"""


def calculate_before_after_lift(
    spend_before_campaign: float,
    spend_during_campaign: float,
) -> float:
    """Calculate naive before/after lift.

    This is simple, but weak, because it does not account for
    seasonality, holidays, category trends, or other external factors.
    """
    return spend_during_campaign - spend_before_campaign


def calculate_test_control_lift_per_user(
    test_group_spend: float,
    test_group_users: int,
    control_group_spend: float,
    control_group_users: int,
) -> float:
    """Calculate lift per user using a test/control comparison."""
    test_avg_spend = test_group_spend / test_group_users
    control_avg_spend = control_group_spend / control_group_users
    return test_avg_spend - control_avg_spend


def calculate_incremental_revenue(
    lift_per_user: float,
    test_group_users: int,
) -> float:
    """Scale lift per user across the test group."""
    return lift_per_user * test_group_users


def classify_incrementality_quality(
    lift_per_user: float,
    reward_cost_per_user: float,
) -> str:
    """Classify whether lift appears strong relative to reward cost.

    This is intentionally simple for Day 5.
    Later, we will use richer rules.
    """
    if lift_per_user <= 0:
        return "no_lift"

    if lift_per_user < reward_cost_per_user:
        return "weak_lift_possible_cannibalization"

    return "strong_lift"


def main() -> None:
    """Run two small examples."""

    print("Scenario 1: Naive Before/After")
    print("------------------------------")
    spend_before_campaign = 80_000.00
    spend_during_campaign = 120_000.00

    naive_lift = calculate_before_after_lift(
        spend_before_campaign=spend_before_campaign,
        spend_during_campaign=spend_during_campaign,
    )

    print(f"Naive before/after lift: ${naive_lift:,.2f}")
    print("This looks good, but it may include seasonality or other external effects.")
    print()

    print("Scenario 2: Test/Control")
    print("------------------------")
    test_group_spend = 120_000.00
    test_group_users = 1_000

    control_group_spend = 110_000.00
    control_group_users = 1_000

    reward_cost_per_user = 20.00

    lift_per_user = calculate_test_control_lift_per_user(
        test_group_spend=test_group_spend,
        test_group_users=test_group_users,
        control_group_spend=control_group_spend,
        control_group_users=control_group_users,
    )

    incremental_revenue = calculate_incremental_revenue(
        lift_per_user=lift_per_user,
        test_group_users=test_group_users,
    )

    incrementality_quality = classify_incrementality_quality(
        lift_per_user=lift_per_user,
        reward_cost_per_user=reward_cost_per_user,
    )

    print(f"Test/control lift per user: ${lift_per_user:,.2f}")
    print(f"Estimated incremental revenue: ${incremental_revenue:,.2f}")
    print(f"Reward cost per user: ${reward_cost_per_user:,.2f}")
    print(f"Incrementality quality: {incrementality_quality}")

    print()
    print("Lesson:")
    print(
        "Before/after suggests a $40,000 lift, but test/control suggests "
        "only $10,000 of incremental revenue. The rest may be organic or caused "
        "by external factors."
    )


if __name__ == "__main__":
    main()