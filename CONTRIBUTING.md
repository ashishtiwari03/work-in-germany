# 🤝 Contributing to Work in Germany

Thank you for wanting to help! Here's how you can contribute.

## 🆕 Add a Company

The easiest way to contribute! Open an [issue](../../issues/new?template=add-company.yml) with:

- **Company name**
- **Career page URL**
- **ATS type** (if you know it): Greenhouse, Lever, Workday, SmartRecruiters, Personio, or Custom
- **Office locations in Germany**
- **Do they sponsor visas?** (yes / no / unknown)
- **Working language** (English / German / both)

We'll add it to the scraper config.

## 🐛 Report a Bug

- Dead link? Wrong category? Incorrect visa/language tag?
- Open an [issue](../../issues/new?template=bug-report.yml) with the job URL and what's wrong.

## 📝 Improve Guides

The docs in `docs/` are always evolving. PRs welcome for:

- Updated salary data
- New visa regulation changes
- Better CV/Anschreiben examples
- Translations (German, Hindi, etc.)
- New sections or resources

## 💻 Code Contributions

### Setup

```bash
git clone https://github.com/YOUR_USERNAME/work-in-germany.git
cd work-in-germany
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Adding a New ATS Scraper

1. Create `scripts/sources/your_ats.py`
2. Extend `BaseScraper` class
3. Implement `fetch_jobs()` method
4. Register it in `scripts/sources/__init__.py`
5. Add company entries in `config.yml` with `ats: your_ats`

### Running the Scraper Locally

```bash
# Full scrape (all supported companies)
cd scripts && python scraper.py

# Single company
python scraper.py --company celonis

# Dry run (no file writes)
python scraper.py --dry-run

# Render README from data
python renderer.py
```

### Code Style

- Python 3.12+
- Type hints encouraged
- Docstrings for public functions
- Keep it simple — this is a community project

## 📜 Code of Conduct

Be respectful, be helpful, be constructive. We're all here to help people find jobs.
