import os
import requests
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load credentials
load_dotenv()

# --- Configuration ---
OTX_API_KEY = os.getenv("OTX_API_KEY")
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# --- OTX Data Fetch ---
def fetch_otx_data():
    """
    Fetches threat intelligence data from AlienVault OTX API.
    """
    url = "https://otx.alienvault.com/api/v1/pulses/subscribed"
    headers = {"X-OTX-API-KEY": OTX_API_KEY}
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"[!] Error fetching data from OTX: {e}")
        return []

# --- Neo4j Insertion ---
def add_indicator(tx, indicator_value, indicator_type, report_name):
    """
    Adds indicator and report nodes, and connects them with a relationship.
    """
    query = """
    MERGE (i:Indicator {value: $value})
      ON CREATE SET i.type = $type
    MERGE (r:Report {name: $report_name})
    MERGE (i)-[:MENTIONED_IN]->(r)
    """
    tx.run(query, value=indicator_value, type=indicator_type, report_name=report_name)

def store_in_neo4j(indicators):
    """
    Connects to Neo4j and stores fetched data.
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    with driver.session() as session:
        for pulse in indicators:
            report_name = pulse.get("name", "Unknown Report")
            for indicator in pulse.get("indicators", []):
                value = indicator.get("indicator")
                type_ = indicator.get("type")
                if value and type_:
                    session.execute_write(add_indicator, value, type_, report_name)
                    print(f"[+] Added Indicator: {value} ({type_})")
    driver.close()

# --- Main Execution ---
if __name__ == "__main__":
    print("[*] Fetching data from AlienVault OTX...")
    data = fetch_otx_data()
    if data:
        print(f"[*] Retrieved {len(data)} pulses. Storing to Neo4j...")
        store_in_neo4j(data)
        print("[✓] Data ingestion completed successfully.")
    else:
        print("[!] No data retrieved from OTX.")
