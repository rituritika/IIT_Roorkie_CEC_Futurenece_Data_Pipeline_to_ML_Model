# Lead Intelligence System

## 1. Project Overview

The Lead Intelligence System is a Python-based command-line application developed for a B2B SaaS sales use case. It automates lead qualification by evaluating CSV records against a transparent scoring rubric, assigning qualification decisions and outreach priorities, and generating personalized first-contact messages using the Groq LLM API.

The objective is to help sales teams identify promising leads, flag uncertain records for human review, and reduce manual qualification effort.

## 2. Key Features

- CSV ingestion and validation.
- Transparent five-factor lead scoring (0–10).
- Automated decisions: QUALIFIED, REVIEW and REJECTED.
- HIGH, MEDIUM and LOW priority classification.
- Evidence-based reasoning for every lead.
- Groq LLM integration with batch processing.
- Personalized outreach messages for qualified leads.
- Retry handling and deterministic fallback when API calls fail.
- JSON reports with individual lead results and aggregate statistics.
- Supports processing 100+ leads in one run.

## 3. Qualification Methodology

Each lead is evaluated using five factors. Each factor carries a
maximum of 2 points, giving a total qualification score of 10.

### Scoring Factors

**1. Company Size Fit (0–2 points)**
- 2 points: 11–5,000 employees
- 1 point: Near-target company size
- 0 points: Outside target range or missing information

**2. Industry Fit (0–2 points)**
- 2 points: Matches a provisional target industry
- 1 point: Other known industries
- 0 points: Missing industry information

**3. Lead Source / Intent (0–2 points)**
- 2 points: Demo requests, referrals or sales calls
- 1 point: Other known sources
- 0 points: Missing source information

**4. Engagement Recency (0–2 points)**
- 2 points: Interaction within the last 30 days
- 1 point: Interaction 31–180 days ago
- 0 points: Interaction older than 180 days or missing

**5. Data Completeness (0–2 points)**
- 2 points: All six required fields populated
- 1 point: 1–2 fields missing
- 0 points: More than 2 fields missing

### Decision Thresholds

| Score | Decision |
|---|---|
| 7–10 | QUALIFIED |
| 4–6 | REVIEW |
| 0–3 | REJECTED |

Missing or uncertain critical information triggers human review.

### Priority Classification

- **HIGH:** Qualified leads scoring 8 or above with interaction within 90 days.
- **MEDIUM:** Other qualified leads and all review leads.
- **LOW:** Rejected leads.

**Note:** These scoring rules are project assumptions, not
trainer-provided ground-truth labels.
## 4. Technology Stack

- Python 3
- CSV and JSON for input and output
- Requests library for HTTP/API communication
- Groq LLM API for batch reasoning and personalized outreach
- Command-line execution

The application uses the default Groq model `llama-3.3-70b-versatile` and processes leads in batches of 10 by default.

## 5. Project Structure

| File | Purpose |
|---|---|
| `main.py` | Lead scoring, LLM integration, error handling and report generation |
| `requirements.txt` | Python dependencies |
| `leads_training.csv` | Training and development dataset |
| `leads_testing.csv` | Edge-case and validation dataset |
| `leads_full_scale_100.csv` | 100-lead demonstration dataset |
| `training_report.json` | Training execution report |
| `testing_report.json` | Testing execution report |
| `output_report.json` | Full-scale execution report |
| `DATA_README.md` | Dataset descriptions and column details |
| `EXPECTED_OUTCOMES_NOTE.md` | Project assumptions and expected outcomes |
| `DELIVERY_SUMMARY.md` | Project delivery summary |

## 6. Setup and Execution

### Install dependencies

```bash
python -m pip install -r requirements.txt
```

### Configure the Groq API key

Set the API key as an environment variable. Never hard-code or commit the actual key.

For GitHub Codespaces / Linux:

```bash
export GROQ_API_KEY="your_groq_api_key"
```

For Windows PowerShell:

```powershell
$env:GROQ_API_KEY="your_groq_api_key"
```

### Run the application

Training dataset:

```bash
python main.py --input leads_training.csv --output training_report.json
```

Testing dataset:

```bash
python main.py --input leads_testing.csv --output testing_report.json
```

Full-scale 100-lead dataset:

```bash
python main.py --input leads_full_scale_100.csv --output output_report.json
```

Offline execution without the LLM:

```bash
python main.py --input leads_testing.csv --output testing_report.json --no-llm
```

## 7. LLM Integration and Error Handling

The application integrates with the Groq API and sends leads in batches rather than making one API request per lead.

- Default batch size: 10 leads.
- Retries transient API failures, rate limits and server errors.
- Uses a request timeout.
- Falls back to local deterministic reasoning if the API is unavailable.
- Preserves locally calculated scores and qualification decisions.
- Uses the LLM to enrich reasoning and generate personalized outreach for qualified leads only.

If the API is unavailable or no key is configured, the application can still generate its report using local reasoning. LLM-generated outreach messages may then be unavailable.

## 8. Output Reports and Sample Results

Each JSON report contains:

- Total number of leads processed.
- Individual qualification scores and decisions.
- Factor-level scoring evidence.
- Reasoning and missing-field information.
- Priority queue.
- Qualified, review and rejected counts.
- Common rejection reasons.
- Up to five sample outreach messages when available.
- API execution mode.

**100-lead demonstration result:**

- Total leads processed: 100
- Qualified leads: 59
- Review leads: 41
- Personalized outreach messages generated: 59

These figures represent the recorded demonstration run. Results can vary with the input data and execution.

## 9. Limitations and Future Improvements

- The scoring rubric is a transparent baseline and is not a statistically validated prediction model.
- Industry and company-size assumptions should be reviewed against the actual business requirements.
- Lead source is only an indicator of possible intent, not proof of purchase readiness.
- Missing or incorrect input data can affect qualification decisions.
- LLM-generated reasoning and messages should be reviewed before use.
- Future enhancements could include CRM integration, configurable scoring weights, additional validation and sales-outcome feedback.

## 10. Security and Submission Checklist

- API keys are maintained as environment secrets and must not be committed.
- No passwords or credentials should be included in the project ZIP.
- Source code, input datasets, generated reports and documentation should be included.
- Temporary files, caches, virtual environments and unnecessary intermediate files should be excluded.

**Project Deliverable:** A working, batch-enabled Lead Intelligence System that processes CSV leads, applies explainable qualification logic, integrates an LLM API, handles failures, and generates actionable JSON reports for sales review.