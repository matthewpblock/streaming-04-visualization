# Project Reflection & Modifications

## What did you observe before your modification?
The original project featured a single, basic line chart tracking raw total sales over message offsets. While it successfully visualized the data in motion, it lacked deeper analytical insights (such as smoothing out erratic data or grouping by category). Furthermore, while the consumer validated incoming messages against a data contract, rejected messages were merely logged and skipped. There was no robust observability or secondary storage for failures.

## What did you decide to modify and why?
I decided to implement a dual-subplot visualization and a Dead Letter Queue (DLQ).
1. **Dual Subplot Visualization:** I added a 5-message moving average to the raw sales line chart and created a second bar chart showing cumulative sales by region. I chose this to provide richer visual analytics—combining trend analysis with categorical aggregation.
2. **Dead Letter Queue (DLQ):** I modified the consumer to act as a producer for invalid messages, routing them to a DLQ topic. I chose this to improve system reliability, ensuring data engineers can inspect and replay rejected messages later.

## Describe your modification using specifics:
- **Visualization (`live_visualizations_critical_section.py`):** Changed the Matplotlib layout to use `plt.subplots(2, 1)`. The top subplot tracks raw sales (`y_values`) and a dynamically calculated 5-message moving average (`ma_values`). The bottom subplot aggregates totals by `region_id` into a dictionary (`region_sales`) and visualizes them using `ax.bar()`.
- **Dead Letter Queue (`kafka_consumer_critical_section.py`):** Imported `create_producer` and `produce_kafka_message`. Initialized a DLQ producer targeting the topic `streaming-04-visualization-critical-section-dlq`. Any message failing `validate_required_fields` is now explicitly produced to this topic before the loop continues.
- **State Management:** Threaded the new visualization variables (`ma_values`, `region_sales`) through the initialization, processing, and charting functions, and updated the Ruff docstrings (`D417`) to pass stringent pre-commit hooks.

## What did you observe after your modification?
When running the consumer, a two-panel interactive Matplotlib window now appears. The top panel plots the live stream with a smoothed red moving average trendline, while the bottom panel's categorical bars grow dynamically as different regions make sales. The terminal successfully indicates when messages are processed, and any simulated invalid messages are explicitly logged as routed to the DLQ topic.

## Describe your custom work being specific:
To accomplish this, I:
- Integrated `kafka_producer_utils.py` directly into the consumer file to grant it publisher capabilities.
- Handled Matplotlib's subplot axes by unpacking them `ax1, ax2 = axes`, calling `.clear()` on each during real-time updates, and plotting `.plot()` for the line charts and `.bar()` for the regional distribution.
- Adjusted `.pre-commit-config.yaml` to exclude the `data/output/` directory from line-ending checks to avoid formatting conflicts with dynamically generated CSV files.
- Ensured the DLQ producer is cleanly flushed and closed in a nested `try/finally` block alongside the Kafka consumer to prevent memory leaks.

## How difficult was this initial modification?
This modification was moderately difficult, as it spanned data engineering, visualization, and system observability domains.

## Was it more or less difficult than you expected and why?
It was slightly more difficult than expected. The core logic for calculating a moving average or a regional sum is straightforward Python, but integrating it into an existing streaming architecture requires careful state management. Threading the new variables (`ma_values`, `region_sales`) across multiple decoupled functions, managing Matplotlib's interactive `.flush_events()` drawing cycle for subplots, and ensuring the code adhered to strict Ruff docstring linting via `pre-commit` added a layer of professional complexity to the task.
