# MMM Data Contracts and Attribution/MMM Alignment

This document clarifies the data contracts used in the Meiro MMM application and how attribution data aligns (or does not) with MMM modeling.

---

## 1. MMM Data Contract

### 1.1 Schema

The MMM time series schema is defined in `backend/app/dataset.schema.json`:

```json
{
  "title": "MMMTimeSeries",
  "required": ["frequency", "index", "kpi", "spend_channels", "frame"],
  "properties": {
    "frequency": { "enum": ["D", "W"] },           // Daily or Weekly
    "index":     { "array of date strings" },
    "kpi":       { "string" },                     // Column name for target/KPI
    "spend_channels": { "array of strings" },      // Column names for spend
    "covariates": { "array of strings" },          // Optional control variables
    "frame":     { "array of row objects" }
  }
}
```

### 1.2 Supported Formats

**Wide format** (one row per period, spend channels as columns):

| date       | sales | meta_spend | google_spend | tv_spend | price_index | holiday |
|------------|-------|------------|--------------|----------|-------------|---------|
| 2024-01-01 | 10000 | 1200       | 900          | 0        | 1.00        | 0       |

- **date** (or equivalent): period start date
- **KPI column**: e.g. `sales`, `conversions`, `revenue`, `aov`, `profit`
- **Spend columns**: one per channel, numeric
- **Covariates** (optional): control variables (price_index, holiday, seasonality, etc.)

**Tall format** (one row per period-channel-campaign):

| date       | channel    | campaign | spend | impressions | clicks | conversions | revenue |
|------------|------------|----------|-------|-------------|--------|-------------|---------|
| 2024-01-01 | meta_ads   | Brand    | 1200  | 50000       | 1200   | 45          | 9000    |

- Required columns: `date`, `channel`, `campaign`, `spend`
- The engine pivots tall → wide using `channel` (or `channel + campaign`) for MMM input

### 1.3 Model Configuration

MMM model config (`ModelConfig` Pydantic model):

| Field           | Type     | Description                                      |
|-----------------|----------|--------------------------------------------------|
| dataset_id      | str      | ID of the dataset                                |
| frequency       | str      | `"D"` or `"W"`                                   |
| kpi_mode        | str      | `"conversions"`, `"aov"`, or `"profit"`          |
| kpi             | str      | Column name for target                           |
| spend_channels  | List[str]| Column names for spend (must match dataset)      |
| covariates      | List[str]| Optional control column names                    |
| priors          | object   | Adstock (alpha) and saturation (lam) priors      |
| mcmc            | object   | draws, tune, chains, target_accept               |

### 1.4 MMM Input → Output

- **Input**: `df[date_column]`, `df[target_column]`, `df[channel_columns]`, optional `df[control_columns]`
- **Output**: `r2`, `contrib` (channel contributions), `roi` (per channel), `uplift`, `engine` (bayesian/ridge)

---

## 2. Attribution Data Contract

### 2.1 Journey Format

Attribution operates on **customer journeys** — ordered sequences of touchpoints with a conversion outcome.

**Required structure** (per journey):

| Field            | Type   | Required | Description                                      |
|------------------|--------|----------|--------------------------------------------------|
| customer_id      | str    | Yes*     | Profile/customer identifier                      |
| touchpoints      | array  | Yes      | Ordered list of touchpoint objects               |
| conversion_value | float  | Yes      | Value of the conversion (e.g. revenue)           |
| converted        | bool   | No       | Default `true`; `false` for non-converters       |

*Also accepted: `profile_id`, `id`

### 2.2 Touchpoint Format

| Field     | Type | Required | Description                                   |
|-----------|------|----------|-----------------------------------------------|
| channel   | str  | Yes      | Channel name (used for attribution & taxonomy)|
| timestamp | str  | No       | ISO date/time for time-based attribution      |
| campaign  | str  | No       | Campaign identifier                           |
| source    | str  | No       | For taxonomy mapping (utm_source)             |
| medium    | str  | No       | For taxonomy mapping (utm_medium)             |

### 2.3 Attribution Models

| Model          | Description                                   |
|----------------|-----------------------------------------------|
| last_touch     | 100% credit to last touchpoint                |
| first_touch    | 100% credit to first touchpoint               |
| linear         | Equal credit across touchpoints               |
| time_decay     | More credit to recent touchpoints (configurable half-life) |
| position_based | U-shaped: first + last get configurable %     |
| markov         | Data-driven (Markov chain removal effect)     |

### 2.4 Attribution Output

- **channel_credit**: `{ channel: attributed_value }`
- **total_conversions**, **total_value**
- **channels**: list with `channel`, `credit`, `share`, `conversions`, `value`

---

## 3. Channel Taxonomy

Channel names are normalized via `app/utils/taxonomy.py` to keep attribution and downstream reporting consistent.

**Default rules** (source/medium regex → channel):

| Channel      | Source regex                         | Medium regex              |
|--------------|--------------------------------------|---------------------------|
| paid_search  | google, bing, baidu                  | cpc, ppc, paid_search     |
| paid_social  | facebook, meta, instagram, linkedin… | cpc, paid_social, social  |
| email        | —                                    | email                     |
| direct       | —                                    | none, direct              |

Sample data uses **raw channel names** (e.g. `google_ads`, `meta_ads`, `linkedin_ads`, `email`, `whatsapp`, `direct`) that may or may not match taxonomy rules. If taxonomy does not match, the raw `channel` value is preserved.

**Important**: Attribution channels and MMM spend channel names are not automatically reconciled. Manual alignment is required (see §5).

---

## 4. Conversion Definitions (ModelConfig)

Versioned model configs (`config_json`) define:

### 4.1 Eligible Touchpoints

```json
"eligible_touchpoints": {
  "include_channels": ["paid_search", "paid_social", "email", "affiliate"],
  "exclude_channels": ["direct"],
  "include_event_types": ["ad_click", "ad_impression", "email_click", "site_visit"],
  "exclude_event_types": []
}
```

### 4.2 Time Windows

```json
"windows": {
  "click_lookback_days": 30,
  "impression_lookback_days": 7,
  "session_timeout_minutes": 30,
  "conversion_latency_days": 7
}
```

### 4.3 Conversion Definitions

```json
"conversions": {
  "primary_conversion_key": "purchase",
  "conversion_definitions": [
    {
      "key": "purchase",
      "name": "Purchase",
      "event_name": "order_completed",
      "filters": [{ "field": "currency", "op": "in", "value": ["EUR", "CZK"] }],
      "value_field": "revenue",
      "dedup_mode": "order_id",
      "attribution_model_default": "data_driven"
    }
  ]
}
```

`primary_conversion_key` is used to annotate journeys with `kpi_type` and to filter which conversions drive attribution when a config is applied.

---

## 5. Attribution / MMM Alignment

### 5.1 Data Lineage

| Source            | Attribution                     | MMM                                  |
|-------------------|---------------------------------|--------------------------------------|
| Conversion paths  | ✓ Journey-level touchpoints     | ✗ Not used directly                  |
| Spend / expenses  | ✓ For ROI = attributed_value ÷ spend | ✓ Spend columns as model inputs |
| Meiro CDP         | ✓ Via conversion paths          | ✓ Via `fetch_and_transform` → weekly aggregation |
| Uploaded CSV      | ✗ Not used                      | ✓ Via DatasetWizard / DatasetUploader|
| Ad platform APIs  | ✗ Not used                      | ✓ Expenses auto-imported for ROI     |

### 5.2 Spend Sources

- **Attribution ROI**: Uses `EXPENSES` (in-memory `{channel}_{period}`) and manual entries from ExpenseManager; ad platform APIs (Meta, LinkedIn) populate channel spend.
- **MMM**: Uses spend columns in the dataset (uploaded or from Meiro CDP). These are **independent** from `EXPENSES` unless you explicitly build the MMM dataset from the same expense data.

### 5.3 Channel Name Alignment

**Gap**: Attribution uses channels from touchpoints (e.g. `google_ads`, `meta_ads`). MMM uses column names you choose when mapping the dataset (e.g. `meta_spend`, `google_spend`). There is no built-in mapping table.

**Recommendation**: Use a consistent channel naming convention:
- Touchpoints: `google_ads`, `meta_ads`, `linkedin_ads`, `email`, `whatsapp`, …
- MMM spend columns: `google_spend`, `meta_spend`, `linkedin_spend`, …
- Expenses: `channel` = `meta_ads`, `google_ads`, etc., matching touchpoint channels so ROI computes correctly.

### 5.4 Conversion ↔ KPI Alignment

- **Attribution**: `conversion_value` and `kpi_type` per journey; `primary_conversion_key` selects which conversions count.
- **MMM**: Single KPI column (e.g. `conversions`, `revenue`, `sales`) at period level.

To align:
- If MMM KPI = `conversions`, aggregate attributed conversions by period to compare.
- If MMM KPI = `revenue`, sum `conversion_value` for attributed conversions; ensure `conversion_value` matches the KPI definition.

### 5.5 Granularity

| Aspect    | Attribution              | MMM                     |
|-----------|--------------------------|-------------------------|
| Granularity | Journey (individual)   | Period (daily/weekly)   |
| Time      | Touchpoint timestamps   | Period start date       |
| Channels  | Per touchpoint          | Per spend column        |
| Output    | Credit per channel      | Contribution per channel|

Aggregation from journey-level attribution to period-level is **not** automated. Path aggregates (`path_aggregates` table) support analytics but are not wired to MMM input.

---

## 6. Conversion Path Storage

### 6.1 ConversionPath Table

| Column         | Type   | Description                              |
|----------------|--------|------------------------------------------|
| conversion_id  | str    | Unique conversion identifier             |
| profile_id     | str    | Customer/profile ID                      |
| conversion_key | str    | e.g. `purchase`, `lead` (from kpi_type)  |
| conversion_ts  | datetime | Conversion timestamp                   |
| path_json     | JSON   | Full journey dict                        |
| path_hash     | str    | SHA256 of channel sequence               |
| length        | int    | Touchpoint count                         |
| first_touch_ts| datetime | First touchpoint timestamp             |
| last_touch_ts | datetime | Last touchpoint timestamp              |

### 6.2 PathAggregate Table

Pre-aggregated by date and path_hash for analytics (path frequency, time-to-convert). Not used for MMM fitting.

---

## 7. Meiro CDP → MMM Bridge

`connectors/meiro_cdp.py`:

1. Exports event/customer data from Meiro CDP.
2. Normalizes date, channel, campaign columns.
3. Aggregates to **weekly** rows: `date`, `channel`, `campaign`, `spend`, `impressions`, `clicks`, `conversions`, `revenue`.

Result is **tall format**, suitable for MMM (engine converts to wide). Column expectations:

- Date: `date`, `timestamp`, `event_date`, or `created_at`
- Channel: `channel`, `source`, `utm_source`, or `traffic_source`
- Campaign: `campaign`, `utm_campaign`, or `campaign_name`
- Metrics: `spend`, `impressions`, `clicks`, `conversions`, `revenue` (numeric)

---

## 8. Summary: What to Align

| Goal                            | Action                                                   |
|---------------------------------|----------------------------------------------------------|
| Compare attribution vs MMM ROI  | Use same channel names in touchpoints, expenses, MMM cols|
| Use CDP data for both          | Export CDP → conversion paths for attribution; same export aggregated for MMM |
| Consistent conversion definition| Set `primary_conversion_key` and ensure value_field matches MMM KPI |
| Reliable channel taxonomy      | Configure `data/taxonomy.json` and normalize touchpoints |

---

## 9. References

- **MMM schema**: `backend/app/dataset.schema.json`
- **Attribution engine**: `backend/app/attribution_engine.py`
- **MMM engine**: `backend/app/mmm_engine.py`
- **Conversion path services**: `backend/app/services_conversions.py`
- **Channel taxonomy**: `backend/app/utils/taxonomy.py`
- **Model config**: `backend/app/models_config_dq.py`, `backend/app/services_model_config.py`
