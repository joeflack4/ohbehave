# Boilerplate for Python Dash Projects

Uses the following packages.
![Python](https://img.shields.io/badge/Python-^3.8-blue.svg?logo=python&longCache=true&logoColor=white&colorB=5e81ac&style=flat-square&colorA=4c566a)
![Flask](https://img.shields.io/badge/Flask-1.1.2-blue.svg?longCache=true&logo=flask&style=flat-square&logoColor=white&colorB=5e81ac&colorA=4c566a)
![Flask-Assets](https://img.shields.io/badge/Flask--Assets-v2.0-blue.svg?longCache=true&logo=flask&style=flat-square&logoColor=white&colorB=5e81ac&colorA=4c566a)
![Pandas](https://img.shields.io/badge/Pandas-v^1.1.5-blue.svg?longCache=true&logo=python&longCache=true&style=flat-square&logoColor=white&colorB=5e81ac&colorA=4c566a)
![Dash](https://img.shields.io/badge/Dash-v1.18.0-blue.svg?longCache=true&logo=python&longCache=true&style=flat-square&logoColor=white&colorB=5e81ac&colorA=4c566a)
![Plotly](https://img.shields.io/badge/Plotly-v4.14.0-blue.svg?longCache=true&logo=python&longCache=true&style=flat-square&logoColor=white&colorB=5e81ac&colorA=4c566a)

Inspired by: [toddbirchard/plotlydash-flask-tutorial](https://github.com/toddbirchard/plotlydash-flask-tutorial).

## Getting Started

### 1. Environment Variables

Create an `.env` file in the root directory with the following variables.

* `FLASK_APP`: Entry point of your application; should be `ohbehave/app.py`.
* `FLASK_ENV`: The environment in which to run your application; either `development` or `production`.
* `SECRET_KEY`: Randomly generated string of characters used to encrypt your app's data.
* `ASSETS_DEBUG` *(optional)*: Debug asset creation and bundling in `development`.
* `COMPRESSOR_DEBUG` *(optional)*: Debug asset compression while in `development`.

### 2. Installation
Run: `pip install -r requirements-unlocked.txt`

### 3. Running
Run: `make deploy`. Then, open 0.0.0.0:5050 in your browser.

#### Running: Production
Needs `pip install uWSGI` (TODO: Need to add more notes here)
