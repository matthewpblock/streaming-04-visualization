"""src/streaming/visualizations/live_visualizations_case.py.

Project-specific live visualization functions used by the Kafka consumer.

This module creates a live line chart of sale total by message.
The chart opens in a window while the consumer is running and updates
as each message is consumed.

Author: Denise Case, Matthew Block
Date: 2026-05

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it live_visualizations_yourname.py,
  and modify your copy for your own charts.
"""

# === DECLARE IMPORTS ===

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

# === DECLARE EXPORTS ===

# Use the built-in __all__ variable to declare a list of
# public objects that this module exports.
# This is a common Python convention that helps other developers understand
# which functions are intended for use outside this module.

__all__ = [
    "close_live_chart",
    "init_live_chart",
    "save_live_chart",
    "update_live_chart",
]


# === DEFINE LIVE CHART HELPERS ===


def init_live_chart() -> tuple[
    Any, Any, list[int], list[float], list[float], dict[str, float]
]:
    """Create and show an empty live chart.

    Returns:
        A tuple of (figure, axes, x_values, y_values, ma_values, region_sales).
    """
    # Matplotlib has a ion() function built in for "interactive ON" mode,
    # which allows the chart to update in real time as we modify it.
    # Call this function to turn on interactive mode.
    plt.ion()

    # Call subplots() to create a figure and axis for the chart.
    figure, axes = plt.subplots(2, 1, figsize=(8, 10))
    figure.subplots_adjust(hspace=0.4)

    # Initialize empty lists for x and y values.
    # These will be updated as messages are consumed.
    x_values: list[int] = []
    y_values: list[float] = []
    ma_values: list[float] = []
    region_sales: dict[str, float] = {}

    # Set the title and axis labels for the chart.
    axes[0].set_title("Sales & 5-Message Moving Average")
    axes[0].set_xlabel("Message")
    axes[0].set_ylabel("Sale Total ($)")

    axes[1].set_title("Cumulative Sales by Region")
    axes[1].set_xlabel("Region")
    axes[1].set_ylabel("Total Sales ($)")

    # Call the figure.show() method to display the chart window.
    figure.show()

    # Call the figure.canvas.draw() method to
    # ensure the chart is rendered and responsive.
    figure.canvas.draw()

    # Call the figure.canvas.flush_events() method to process any pending GUI events,
    # which helps the chart window to update properly.
    figure.canvas.flush_events()

    # Return the figure, axis, and the x and y value lists for later use.
    return figure, axes, x_values, y_values, ma_values, region_sales


def update_live_chart(
    *,
    figure: Any,
    axis: Any,
    x_values: list[int],
    y_values: list[float],
    ma_values: list[float],
    region_sales: dict[str, float],
    message: dict[str, Any],
) -> None:
    """Update the live chart with one consumed message.

    All arguments after the asterisk (*) must be passed as keyword arguments.

    Arguments:
        figure: Matplotlib figure.
        axis: Matplotlib axis.
        x_values: List of x-axis values already shown.
        y_values: List of y-axis values already shown.
        ma_values: List of moving average values.
        region_sales: Dictionary tracking cumulative sales per region.
        message: One enriched Kafka message dictionary.


    Returns:
        None.
    """
    # The message offset is a unique integer
    # that increments with each message,
    # so it works great as a simple x-axis value
    # to show the order of messages.
    # Create a new x value from the message offset.
    new_x = int(message["_kafka_offset"])
    x_values.append(new_x)

    new_y = float(message["total"])
    y_values.append(new_y)

    # Calculate 5-message moving average
    window = y_values[-5:]
    ma_values.append(sum(window) / len(window))

    # Update regional cumulative sales
    region = message.get("region_id", "Unknown")
    region_sales[region] = region_sales.get(region, 0.0) + new_y

    # Unpack the two subplot axes
    ax1, ax2 = axis
    ax1.clear()
    ax2.clear()

    # Plot Top Subplot: Raw Sales and Moving Average
    ax1.plot(x_values, y_values, marker="o", alpha=0.3, label="Raw Total")
    ax1.plot(x_values, ma_values, marker="", color="red", linewidth=2, label="5-Msg MA")
    ax1.set_title("Sales & 5-Message Moving Average")
    ax1.set_xlabel("Message")
    ax1.set_ylabel("Sale Total ($)")
    ax1.legend()
    ax1.grid(True)

    # Plot Bottom Subplot: Cumulative Bar Chart
    regions = list(region_sales.keys())
    sales = list(region_sales.values())
    ax2.bar(regions, sales, color="skyblue")
    ax2.set_title("Cumulative Sales by Region")
    ax2.set_xlabel("Region")
    ax2.set_ylabel("Total Sales ($)")
    ax2.grid(True, axis='y')

    # Call the figure.canvas.draw() method to update the chart with the new data.
    figure.canvas.draw()

    # Call the figure.canvas.flush_events() method to process any pending GUI events,
    # which helps the chart to update properly.
    figure.canvas.flush_events()

    # Call plt.pause() with a short time (e.g., 0.05 seconds) to allow the chart to update.
    plt.pause(0.05)


def save_live_chart(
    *,
    figure: Any,
    chart_path: Path,
) -> None:
    """Save the final live chart to an image file.

    All arguments after the asterisk (*) must be passed as keyword arguments.

    Arguments:
        figure: Matplotlib figure.
        chart_path: Output image path.

    Returns:
        None.
    """
    # Ensure the output directory exists before saving the figure.
    chart_path.parent.mkdir(parents=True, exist_ok=True)

    # Use the figure.savefig() method to save the chart to an image file.
    # Use the bbox_inches="tight" argument to ensure the saved image is cropped to the content of the chart.
    figure.savefig(chart_path, bbox_inches="tight")


def close_live_chart() -> None:
    """Turn off interactive chart mode."""
    # Call plt.ioff() to turn off interactive mode when the consumer is finished.
    plt.ioff()
