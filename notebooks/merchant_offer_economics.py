"""
This script uses tiny made-up numbers so I can understand
the formulas before implementing them later in Spark and dbt.
"""


def calculate_lift_per_user(
    test_group_spend: float,
    test_group_users: int,
    control_group_spend: float,
    control_group_users: int,
) -> float:
    """Calculate spend lift per user.

    Lift tells us how much more the average test-group user spent
    compared with the average control-group user.

    Args:
        test_group_spend: Total spend from users who received/activated/redeemed the offer.
        test_group_users: Number of users in the test group.
        control_group_spend: Total spend from similar users who did not receive the offer.
        control_group_users: Number of users in the control group.

    Returns:
        The estimated lift per user.
    """
    test_avg_spend = test_group_spend / test_group_users
    control_avg_spend = control_group_spend / control_group_users
    return test_avg_spend - control_avg_spend


def calculate_incremental_revenue(
    lift_per_user: float,
    test_group_users: int,
) -> float:
    """Scale per-user lift across the test group."""
    return lift_per_user * test_group_users


def calculate_net_merchant_profit(
    incremental_revenue: float,
    merchant_margin_rate: float,
    total_reward_liability: float,
    platform_fees: float,
) -> float:
    """Calculate profit after margin, rewards, and platform fees."""
    gross_profit_from_lift = incremental_revenue * merchant_margin_rate
    return gross_profit_from_lift - total_reward_liability - platform_fees


def calculate_roas(
    incremental_revenue: float,
    total_reward_liability: float,
) -> float:
    """Calculate return on reward/ad spend."""
    if total_reward_liability == 0:
        return 0.0

    return incremental_revenue / total_reward_liability


def classify_cannibalization_risk(
    lift_per_user: float,
    net_merchant_profit: float,
) -> str:
    """Classify whether the campaign may be cannibalistic.

    This is intentionally simple for Day 4.
    Later, we will make this richer using multiple features.
    """
    if net_merchant_profit < 0 and lift_per_user <= 5:
        return "high"

    if net_merchant_profit < 0:
        return "medium"

    return "low"


def main() -> None:
    """Run one sample merchant-offer economics scenario."""
    test_group_spend = 120_000.00
    test_group_users = 1_000

    control_group_spend = 90_000.00
    control_group_users = 1_000

    merchant_margin_rate = 0.40
    total_reward_liability = 7_000.00
    platform_fees = 1_000.00

    lift_per_user = calculate_lift_per_user(
        test_group_spend=test_group_spend,
        test_group_users=test_group_users,
        control_group_spend=control_group_spend,
        control_group_users=control_group_users,
    )

    incremental_revenue = calculate_incremental_revenue(
        lift_per_user=lift_per_user,
        test_group_users=test_group_users,
    )

    net_merchant_profit = calculate_net_merchant_profit(
        incremental_revenue=incremental_revenue,
        merchant_margin_rate=merchant_margin_rate,
        total_reward_liability=total_reward_liability,
        platform_fees=platform_fees,
    )

    roas = calculate_roas(
        incremental_revenue=incremental_revenue,
        total_reward_liability=total_reward_liability,
    )

    cannibalization_risk = classify_cannibalization_risk(
        lift_per_user=lift_per_user,
        net_merchant_profit=net_merchant_profit,
    )

    print("MerchantLift BI Economics Practice")
    print("----------------------------------")
    print(f"Lift per user: ${lift_per_user:,.2f}")
    print(f"Incremental revenue: ${incremental_revenue:,.2f}")
    print(f"Net merchant profit: ${net_merchant_profit:,.2f}")
    print(f"ROAS: {roas:,.2f}x")
    print(f"Cannibalization risk: {cannibalization_risk}")


if __name__ == "__main__":
    main()