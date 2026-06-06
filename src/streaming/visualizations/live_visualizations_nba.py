"""src/streaming/visualizations/live_visualizations_nba.py.

Creates a live dual-panel chart of NBA events.
"""

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

__all__ = [
    "close_live_chart",
    "init_live_chart",
    "save_live_chart",
    "update_live_chart",
]


def init_live_chart() -> tuple[Any, Any, dict[str, dict[str, int]]]:
    """Create and show an empty live chart for NBA events."""
    plt.ion()
    figure, axis = plt.subplots(1, 1, figsize=(8, 6))

    team_stats = {
        "OKC": {"Rebounds": 0, "Fouls": 0, "Turnovers": 0},
        "SAS": {"Rebounds": 0, "Fouls": 0, "Turnovers": 0},
    }

    axis.set_title("Team Stats: Rebounds, Fouls, and Turnovers")
    axis.set_ylabel("Count")
    axis.set_ylim(0, 80)

    figure.show()
    figure.canvas.draw()
    figure.canvas.flush_events()

    return figure, axis, team_stats


def update_live_chart(
    *,
    figure: Any,
    axis: Any,
    team_stats: dict[str, dict[str, int]],
    player_to_team: dict[str, str],
    message: dict[str, Any],
) -> None:
    """Update the live chart with one consumed NBA message."""
    action_type = message.get("action_type", "")
    player_id = message.get("player_id", "")

    team = player_to_team.get(player_id, "Unknown")

    # Ignore events that do not correlate to tracked teams
    if team not in team_stats:
        return

    if action_type == "Rebound":
        team_stats[team]["Rebounds"] += 1
    elif action_type == "Foul":
        team_stats[team]["Fouls"] += 1
    elif action_type == "Turnover":
        team_stats[team]["Turnovers"] += 1

    axis.clear()

    # Parse quarter and time remaining
    time_remaining = message.get("time_remaining", "")
    quarter = message.get("quarter", "?")
    mm_ss = time_remaining
    if time_remaining.startswith("PT") and "M" in time_remaining:
        try:
            m_str, s_str = time_remaining[2:].split("M")
            mm_ss = f"{int(m_str):02d}:{int(float(s_str.replace('S', ''))):02d}"
        except ValueError:
            pass

    metrics = ["Rebounds", "Fouls", "Turnovers"]
    teams = ["OKC", "SAS"]
    colors = {"OKC": "#007AC1", "SAS": "#C4CED4"}  # OKC Blue, Spurs Silver

    x = [0, 1, 2]
    width = 0.35

    # Create clustered bars
    for i, t in enumerate(teams):
        values = [team_stats[t][m] for m in metrics]
        offset = (i - 0.5) * width
        x_pos = [xi + offset for xi in x]
        axis.bar(x_pos, values, width, label=t, color=colors[t])

    axis.set_title("Team Stats: Rebounds, Fouls, and Turnovers")
    axis.set_xticks(x)
    axis.set_xticklabels(metrics)
    axis.set_ylabel("Count")
    axis.set_ylim(0, 45)
    axis.legend(title=f"Q{quarter} {mm_ss}")
    axis.grid(True, axis='y')

    figure.canvas.draw()
    figure.canvas.flush_events()
    plt.pause(0.05)


def save_live_chart(*, figure: Any, chart_path: Path) -> None:
    """Save the live chart to an image file."""
    chart_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(chart_path, bbox_inches="tight")


def close_live_chart() -> None:
    """Close the live chart."""
    plt.ioff()
