# Streaming Data Visualization: Sales and NBA Analytics

This project demonstrates the power of Apache Kafka for real-time streaming analytics by visualizing live data as it is consumed. It features two distinct pipelines: an extended e-commerce sales pipeline that introduces a Dead Letter Queue (DLQ) alongside dual-panel charts for enhanced observability, and a custom NBA play-by-play pipeline that dynamically correlates player actions to team statistics, visualizing rebounds, fouls, and turnovers as a live game clock ticks down.

## Custom Project

### Dataset

For the custom application, I used a completely new dataset rather than the original sales data.
- **File Name:** `events.csv` (accompanied by a reference `players.csv`).
- **Record Type:** Live NBA play-by-play events from a single match.
- **Included Fields:** `play_id`, `game_id`, `timestamp`, `player_id`, `action_type`, `shot_type`, `distance_ft`, `is_made`, `quarter`, and `time_remaining`.

### Kafka Messages

The Kafka producer simulates a live event feed.
- **What is Sent:** JSON-encoded dictionaries representing individual play events (e.g., a rebound or a turnover).
- **Topic:** `streaming-04-nba` (configured to dynamically clear old records on startup to avoid overlapping historic data during tests).
- **Message Key:** The unique `play_id` for each event.
- **Field Modification:** Entirely replaced the original sales schema with the custom NBA data contract to enforce strict requirements.

### Consumer Processing

The consumer receives the raw stream from Kafka and transforms it for the visualization layer.
- **Reception:** Receives individual JSON events up to a maximum limit of 1000 records.
- **Data Enrichment:** Correlates the `player_id` against an in-memory dictionary built from `players.csv` to map actions to their respective teams (OKC or SAS).
- **Filtering:** Actively ignores events that do not belong to the target teams (OKC and SAS) or do not match targeted `action_type` values ("Rebound", "Foul", "Turnover").
- **Logging & Storage:** Logs the accepted `play_id`, assigned team, and action type to the console while simultaneously appending valid, enriched rows into `data/output/consumed_nba_events.csv`.

### Experiments

Two distinct experiments were conducted to extend the project's capabilities:
1. **Phase 4 Change (Sales Pipeline):** Introduced a 5-message moving average into the original sales chart to smooth out volatile trends, and added a Dead Letter Queue (DLQ) to explicitly route and save failed data validations into a separate Kafka topic.
2. **Phase 5 Application (NBA Pipeline):** Built an entirely new sports visualization using clustered column charts. This required state management modifications—such as locking the Y-axis to a fixed max value of 45 to keep the chart scale consistent, and parsing a raw string (`PT11M21.00S`) into a formatted `MM:SS` game clock that ticks actively inside the Matplotlib legend.

### Results

Running the custom NBA producer and consumer successfully simulated an accelerated live game. The producer immediately cleared the topic upon starting, ensuring a fresh data run. As events flowed in at 0.1-second intervals, the consumer mapped players to teams and dynamically raised the blue (OKC) and silver (SAS) bars in the clustered chart. Irrelevant plays were silently bypassed without cluttering the logs, and a PNG image of the final game statistics was successfully saved upon completion.

### Interpretation

Watching the messages stream sequentially through the Kafka architecture revealed several critical insights:
- **Change from Original:** We transitioned from tracking continuous financial metrics (sales) to categorical, discrete events (sports plays). This shift demonstrated the necessity of joining streams with reference data (`players.csv`) prior to plotting.
- **Learning from Kafka:** Kafka's behavior of storing topic history proved immediately obvious when testing; rerunning the producer without clearing the topic compounded the game stats. Adding `prepare_producer_topic()` demonstrated how easily infrastructure state can influence analytical outcomes.
- **Business Value:** A live stream enables organizations to instantly track Key Performance Indicators (KPIs) exactly when they happen. By watching visual representations update dynamically, users are freed from the latency of batch-processing queries.
- **Business Intelligence:** For a sports franchise or analytics firm, tracking momentum shifts, turnover differentials, or accumulating foul trouble in real time empowers immediate tactical decisions—allowing coaches to pivot strategies mid-quarter rather than reacting to a post-game box score.
