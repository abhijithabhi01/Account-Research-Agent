# Account Research Agent (ARA)

**Account Research Agent (ARA)** is an AI-powered B2B company
intelligence platform built with **Google Agent Development Kit (ADK)**
and **Gemini 2.5 Flash**. It autonomously researches a company from
multiple trusted public data sources, validates the information, and
generates a structured account intelligence report for sales, business
development, and market research teams.

## Features

-   AI-powered autonomous account research
-   Single-agent architecture using Google ADK
-   Multi-source data collection
-   Cross-source validation
-   Structured executive research brief
-   Modern responsive web interface
-   Automatic citation of information sources
-   JSON-based intelligence generation

## Architecture

``` text
                User
                  │
                  ▼
          HTML Frontend
                  │
                  ▼
        Flask Backend (Python)
                  │
                  ▼
     Google ADK Account Agent
                  │
     ─────────────────────────────
      Calls Research Tool Functions
     ─────────────────────────────
        │
        ├── Company Website
        ├── Company News
        ├── Job Boards
        ├── Financial Filings
        ├── LinkedIn Signals
        ├── Company Registry
        ├── Tender Opportunities
        └── Industry Directories
                  │
                  ▼
         Gemini 2.5 Flash
                  │
                  ▼
      Executive Research Report
```

## Data Sources

-   Company Website
-   Google News (Serper)
-   Tavily Search
-   Greenhouse Jobs
-   RapidAPI JSearch
-   SEC EDGAR
-   SEC API
-   Financial Modeling Prep (FMP)
-   OpenCorporates
-   LinkedIn Public Pages
-   GeM Tender Portal
-   Crunchbase
-   G2

## Technology Stack

### Backend

-   Python
-   Flask
-   Google ADK
-   Gemini 2.5 Flash
-   BeautifulSoup
-   Requests
-   Python Dotenv

### Frontend

-   HTML5
-   CSS3
-   Vanilla JavaScript

### AI

-   Google ADK
-   Gemini 2.5 Flash
-   Vertex AI

## Project Structure

``` text
ARA/
├── agent.py
├── index.html
├── serviceaccount.json
├── .env
├── requirements.txt
└── README.md
```

## Workflow

1.  User enters company name and website.
2.  Flask backend receives the request.
3.  Google ADK creates the research agent.
4.  Research tools collect information from trusted public sources.
5.  Gemini synthesizes the results.
6.  A structured account intelligence report is generated.

## Report Sections

-   Company Summary
-   Current Initiatives
-   Expansion Signals
-   Hiring Signals
-   Digital Transformation Signals
-   Possible Business Pain Points

## Environment Variables

``` env
SERPER_API_KEY=
TAVILY_API_KEY=
RAPIDAPI_KEY=
FMP_API_KEY=
SEC_API_KEY=
APIFY_API_TOKEN=
```

## Installation

``` bash
git clone https://github.com/yourusername/account-research-agent.git
cd account-research-agent
pip install -r requirements.txt
python agent.py
```

Open:

    http://127.0.0.1:5000

## APIs Used

  Service            Purpose
  ------------------ ---------------------
  Google ADK         Agent orchestration
  Gemini 2.5 Flash   AI reasoning
  Vertex AI          Model hosting
  Serper API         News
  Tavily API         Web search
  Greenhouse API     Jobs
  RapidAPI JSearch   Jobs
  SEC API            Filings
  SEC EDGAR          Public filings
  FMP                Financial metrics
  OpenCorporates     Registry
  Apify              Tender scraping
  Crunchbase         Company profile
  G2                 Product insights

## Future Improvements

-   Multi-agent architecture
-   CRM integration
-   PDF export
-   Deep research mode
-   Competitor analysis
-   Dashboard analytics
