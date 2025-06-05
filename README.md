# Data Bridge

## Description
This project is a data integration pipeline ("bridge") composed of one or more data integration elements ("Streams"). The DIEs consist primarily of a single Python script that calls on utility functions to extract, transform, and load data. A config file should be set by the user to define available data source and destination details, including credentials, ports, connection strings, and so on. In this file, job-specific configuration is also defined specifying which source(s) and destination(s) each job will integratate and dependencies that will be used to facilitate the integration (e.g., query files, remote file paths to source files, etc.). 

## Install in Production Environment
In clean, working development environment:
    pip3 freeze > requirements.txt
In production environment:
    cd /path/to/project/parent
    git clone <repo URL>
    cd <repo name>
    python3 -m venv venv
    MacOS: source venv/bin/activate
    Windows: .\venv\Scripts\activate
    pip3 install -r requirements.txt
    pip3 install . --no-deps
