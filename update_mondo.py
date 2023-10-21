import pandas as pd
from rdflib import Graph, Namespace
from wikibaseintegrator import wbi_login, WikibaseIntegrator, datatypes
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikidataintegrator import wdi_core, wdi_login

from login import USER, PASS
from wikibaseintegrator.models import Qualifiers, References, Reference
from pathlib import Path

login_instance = wbi_login.Clientlogin(user=USER, password=PASS)
wbi = WikibaseIntegrator(login=login_instance)
wbi_config[
    "USER_AGENT"
] = "TiagoLubiana (https://www.wikidata.org/wiki/User:TiagoLubiana)"

# Define SPARQL query to retrieve all items with MONDO references
sparql_query = """
    SELECT ?item ?mondo_id
    WHERE {
        ?item wdt:P5270 ?mondo_id.
    }
"""

# Execute the SPARQL query
result = wdi_core.WDItemEngine.execute_sparql_query(sparql_query)

# Convert result to dataframe
df = pd.json_normalize(result['results']['bindings'])

# Group the dataframe by item and aggregate MONDO ids into lists
grouped_df = df.groupby('item.value')['mondo_id.value'].apply(list).reset_index()

for index, row in grouped_df.iterrows():
    item = wbi.item.get(entity_id=row['item.value'].split("/")[-1])
    
    # List to hold modified MONDO ids
    modified_mondo_ids = [mondo_id.replace("MONDO:", "MONDO_") for mondo_id in row['mondo_id.value']]
    
    # List to hold new statements
    new_statements = [datatypes.String(value=modified_mondo_id, prop_nr="P5270") for modified_mondo_id in modified_mondo_ids]
    
    # Add new statements
    for statement in new_statements:
        item.claims.add(
            statement, action_if_exists=ActionIfExists.REPLACE_ALL
        )
    
    item.write(summary="Fix formatting of MONDO ID")
