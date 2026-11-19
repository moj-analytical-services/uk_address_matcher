import http.client
import os
import shutil
import urllib.request
import zipfile
from urllib.parse import urljoin, urlparse

import duckdb
import pandas as pd
from bs4 import BeautifulSoup
from IPython.display import display

from uk_address_matcher.cleaning.token_and_term_frequencies import (
    get_address_token_frequencies_from_address_table,
    get_numeric_term_frequencies_from_address_table,
)


def get_psc_snapshot_links(url):
    parsed_url = urlparse(url)
    conn = http.client.HTTPSConnection(parsed_url.netloc)
    conn.request("GET", parsed_url.path)
    response = conn.getresponse()

    if response.status != 200:
        raise Exception(f"Request failed with status: {response.status}")

    html_content = response.read()
    soup = BeautifulSoup(html_content, "html.parser")
    li_elements = soup.find_all("li")

    links = []
    for li in li_elements:
        a_tag = li.find("a")
        if a_tag and "of" in a_tag.text:
            full_link = urljoin(url, a_tag["href"])
            links.append(full_link)

    return links


def process_zip_to_parquet(zip_path, parquet_path, temp_dir, file_identifier):
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(temp_dir)
        json_files = [os.path.join(temp_dir, name) for name in zip_ref.namelist()]

        for json_file in json_files:
            path = json_file

            sql = f"""
            with addresses as (
            SELECT
                '{file_identifier}_' || cast(row_number() over() as text) as unique_id,
                company_number,
                data -> 'address' ->> 'premises' AS premises,
                data -> 'address' ->> 'address_line_1' AS address_line_1,
                data -> 'address' ->> 'address_line_2' AS address_line_2,
                data -> 'address' ->> 'country' AS country,
                data -> 'address' ->> 'locality' AS locality,
                data -> 'address' ->> 'postal_code' AS postal_code,
                data -> 'address' ->> 'region' AS region
            FROM read_json_auto('{path}')
            where country in (
            'England',
            'United Kingdom',
            'Scotland',
            'Wales',
            'Northern Ireland',
            'Great Britain'
            ) or country is null
            )
            select
            unique_id,
            concat_ws(' ', premises, address_line_1, address_line_2, locality, region) as address_concat,
            postal_code as postcode
            from addresses
            """
            address_table = con.sql(sql)

            sql = f"""
            COPY (SELECT * FROM address_table)
            TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """
            con.sql(sql)

    os.remove(zip_path)
    for file in json_files:
        os.remove(file)


url = "https://download.companieshouse.gov.uk/en_pscdata.html"
psc_links = get_psc_snapshot_links(url)

temp_dir = os.path.join(os.getcwd(), "temp")
os.makedirs(temp_dir, exist_ok=True)

parquet_out_dir = os.path.join(temp_dir, "parquet_out")
os.makedirs(parquet_out_dir, exist_ok=True)

con = duckdb.connect(database=":memory:")

for link in psc_links:
    zip_name = os.path.basename(urlparse(link).path)
    zip_path = os.path.join(temp_dir, zip_name)

    urllib.request.urlretrieve(link, zip_path)
    print(f"Downloaded: {zip_path}")

    file_identifier = zip_name.split("_")[-1].replace(".zip", "")
    parquet_path = os.path.join(parquet_out_dir, zip_name.replace(".zip", ".parquet"))

    process_zip_to_parquet(zip_path, parquet_path, temp_dir, file_identifier)

    print(f"Processed and saved: {parquet_path}")


for file_name in os.listdir(temp_dir):
    file_path = os.path.join(temp_dir, file_name)
    if file_name != "parquet_out":
        if os.path.isdir(file_path):
            shutil.rmtree(file_path)
        else:
            os.remove(file_path)

sql = """
select count(*) as count
from read_parquet('temp/parquet_out/*.parquet')
"""
count = con.sql(sql).df()
print(f"Total number of addresses: {count['count'][0]:,.0f}")

sql = """
select distinct on (address_concat, postcode) *
from read_parquet('temp/parquet_out/*.parquet')

"""
all_addresses = con.sql(sql).df()

tf_address_tokens = get_address_token_frequencies_from_address_table(
    all_addresses, con=con
)

sql = """
COPY (SELECT * FROM tf_address_tokens)
TO 'address_token_frequencies.parquet' (FORMAT 'parquet', COMPRESSION 'zstd');
"""
con.sql(sql)
tf_num_tokens = get_numeric_term_frequencies_from_address_table(all_addresses, con=con)

sql = """
COPY (SELECT * FROM tf_num_tokens)
TO 'numeric_token_frequencies.parquet' (FORMAT 'parquet', COMPRESSION 'zstd');
"""

con.sql(sql)

sql = """
select count(*) as count from read_parquet('address_token_frequencies.parquet')
limit 10
"""
display(con.sql(sql).df())

sql = """
select count(*) as count from read_parquet('numeric_token_frequencies.parquet')
limit 10
"""
display(con.sql(sql).df())
