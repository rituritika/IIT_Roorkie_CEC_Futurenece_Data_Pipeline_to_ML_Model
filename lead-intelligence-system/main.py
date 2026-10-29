
import csv
import json
import os
import sys
import time
from datetime import date
from pathlib import Path

import requests

API_URL = "https://api.groq.com/openai/v1/chat/completions"
MODEL = "openai/gpt-oss-20b"
BATCH_SIZE = 10


def score_lead(lead):
    """Score a lead using five transparent factors."""
    score = 0
    reasons = []
    missing = []

    size_text = lead.get("company_size", "").strip()
    industry = lead.get("industry", "").strip().lower()
    source = lead.get("source", "").strip().lower()
    interaction = lead.get("last_interaction_date", "").strip()

    try:
        size = int(float(size_text.replace(",", "")))
    except (ValueError, TypeError):
        size = None
        missing.append("company_size")

    if size is not None and 11 <= size <= 5000:
        points = 2
        reasons.append("Company size fits target")
    elif size is not None and 6 <= size <= 8000:
        points = 1
        reasons.append("Company size is near target")
    else:
        points = 0
        reasons.append("Company size is outside target or unknown")
    score += points

    target_industries = {
        "saas", "software", "technology", "fintech", "edtech"
    }
    if industry in target_industries:
        points = 2
        reasons.append("Industry matches provisional target")
    elif industry:
        points = 1
        reasons.append("Industry fit needs confirmation")
    else:
        points = 0
        missing.append("industry")
        reasons.append("Industry is missing")
    score += points

    if any(x in source for x in
           ["demo", "referral", "sales call"]):
        points = 2
        reasons.append("Source suggests direct interest")
    elif source:
        points = 1
        reasons.append("Source suggests some engagement")
    else:
        points = 0
        missing.append("source")
        reasons.append("Lead source is missing")
    score += points

    try:
        interaction_date = date.fromisoformat(interaction[:10])
        age = (date.today() - interaction_date).days
        if age < 0:
            points = 0
            reasons.append("Interaction date is in the future")
        elif age <= 30:
            points = 2
            reasons.append("Interaction is recent")
        elif age <= 180:
            points = 1
            reasons.append("Interaction is moderately old")
        else:
            points = 0
            reasons.append("Interaction is stale")
    except (ValueError, TypeError):
        age = None
        points = 0
        missing.append("last_interaction_date")
        reasons.append("Interaction date is missing or invalid")
    score += points

    required = [
        "name", "company", "company_size",
        "industry", "source", "last_interaction_date"
    ]
    absent = [key for key in required if not lead.get(key, "").strip()]
    if not absent:
        points = 2
        reasons.append("All required fields are present")
    elif len(absent) <= 2:
        points = 1
        reasons.append("Some required fields are missing")
    else:
        points = 0
        reasons.append("Several required fields are missing")
    score += points

    if missing or not lead.get("name", "").strip() \
            or not lead.get("company", "").strip():
        decision = "REVIEW"
    elif 7 <= score <= 10:
        decision = "QUALIFIED"
    elif 4 <= score <= 6:
        decision = "REVIEW"
    else:
        decision = "REJECTED"

    

    priority = (
        "HIGH" if decision == "QUALIFIED" and score >= 8
        else "MEDIUM" if decision in ("QUALIFIED", "REVIEW")
        else "LOW"
    )

    return {
        "qualification_score": score,
        "decision": decision,
        "priority": priority,
        "reasoning": "; ".join(reasons),
        "missing_fields": absent,
        "days_since_interaction": age,
    }


def llm_batch(batch, api_key):
    """One LLM request for a batch of leads."""
    prompt = (
        "For each lead return JSON array items with lead_id, reasoning, "
        "outreach_message. Do not invent facts. Preserve decisions and scores. "
        "Write a personalized message only for QUALIFIED leads; otherwise "
        "return an empty message. Return JSON only. Leads: "
        + json.dumps(batch)
    )
    for attempt in range(3):
        try:
            response = requests.post(
                API_URL,
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "model": MODEL,
                    "temperature": 0.2,
                    "messages": [
                        {"role": "system",
                         "content": "Return valid JSON only."},
                        {"role": "user", "content": prompt},
                    ],
                },
                timeout=45,
            )
            if response.status_code == 429 or response.status_code >= 500:
                raise requests.HTTPError(
                    f"Temporary API error: {response.status_code}"
                )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            content = content.replace("```json", "").replace("```", "").strip()
            return json.loads(content)
        except (requests.RequestException, ValueError, KeyError) as exc:
            if attempt < 2:
                time.sleep(10*(attempt+1))
            else:
                print("LLM batch failed; using local reasoning:", exc)
    return []


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py input.csv [output.json]")
        sys.exit(1)

    input_file = Path(sys.argv[1])
    output_file = Path(
        sys.argv[2] if len(sys.argv) > 2 else "output_report.json"
    )

    with input_file.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        required = {
            "name", "company", "company_size",
            "industry", "source", "last_interaction_date"
        }
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError("Input CSV does not contain all required columns")
        leads = list(reader)

    results = []
    api_key = os.getenv("GROQ_API_KEY", "")

    for start in range(0, len(leads), BATCH_SIZE):
        batch_results = []
        for index, lead in enumerate(
            leads[start:start + BATCH_SIZE], start=start + 1
        ):
            item = {
                "lead_id": index,
                "name": lead.get("name", "").strip(),
                "company": lead.get("company", "").strip(),
                "industry": lead.get("industry", "").strip(),
                "source": lead.get("source", "").strip(),
                **score_lead(lead),
            }
            item["outreach_message"] = ""
            batch_results.append(item)

        if api_key:
            llm_results = llm_batch(batch_results, api_key)
            by_id = {
                str(item.get("lead_id")): item
                for item in llm_results if isinstance(item, dict)
            }
            for item in batch_results:
                extra = by_id.get(str(item["lead_id"]), {})
                if extra.get("reasoning"):
                    item["reasoning"] = extra["reasoning"]
                if item["decision"] == "QUALIFIED":
                    item["outreach_message"] = extra.get(
                        "outreach_message", ""
                    )
        results.extend(batch_results)
        print(f"Processed {min(start + BATCH_SIZE, len(leads))}/{len(leads)}")

    qualified = [x for x in results if x["decision"] == "QUALIFIED"]
    rejected = [x for x in results if x["decision"] == "REJECTED"]
    review = [x for x in results if x["decision"] == "REVIEW"]

    report = {
        "project": "Lead Intelligence System",
        "rubric": "Five factors scored 0-2: company size, industry, "
                  "source/intent, interaction recency, data completeness.",
        "summary": {
            "total_processed": len(results),
            "qualified_count": len(qualified),
            "qualified_percent": round(
                100 * len(qualified) / len(results), 2
            ) if results else 0,
            "review_count": len(review),
            "rejected_count": len(rejected),
        },
        "priority_queue": sorted(
            qualified,
            key=lambda x: x["qualification_score"],
            reverse=True,
        ),
        "sample_outreach_messages": [
            {"name": x["name"], "company": x["company"],
             "message": x["outreach_message"]}
            for x in qualified if x["outreach_message"]
        ][:5],
        "leads": results,
        "llm_enabled": bool(api_key),
    }

    output_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("Report saved to", output_file)


if __name__ == "__main__":
    main()
