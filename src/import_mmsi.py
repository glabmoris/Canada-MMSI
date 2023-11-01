# MMSI import
# 
# @author Glm

import sys
import requests
import mysql.connector
from bs4 import BeautifulSoup

if len(sys.argv) != 4:
	sys.stderr.write("Usage: python3 import_mmsi.py db_host db_user db_password\n")
	sys.exit(1)

# Connect to database
db = mysql.connector.connect(
	database='ais',
	host=sys.argv[1],
	user=sys.argv[2],
	password=sys.argv[3]
)

# Fetch data from Industry Canada
r = requests.get("https://sd.ic.gc.ca/pls/engdoc_anon/mmsi_search.Ship_Search?inMMSI=&inVesName=&inName=&inCallsign=&inVesId=")

if r.status_code == 200:
	sys.stderr.write("Got data...\n")
	htmlData = BeautifulSoup(r.text, 'html.parser')
	tables = htmlData.find(class_="wb-tables")

	if tables:
		# Parse data from HTML
		sys.stderr.write("Datatable found...parsing...\n")
		for tr in tables.find_all("tr"):
			td = tr.find_all("td")

			if td:
				mmsi = td[0].a.get_text()
				ship_name = td[1].get_text()
				vessel_id_number = td[2].get_text()
				call_sign = td[3].get_text()

				# Update / insert data into database
				cursor = db.cursor()
				cursor.execute(f"SELECT * FROM ships where mmsi={mmsi}")

				if len(cursor.fetchall()) == 0:
					sql = "INSERT INTO ships (mmsi,ship_name,vessel_id_number,callsign) VALUES (%s,%s,%s,%s)"
					values = (int(mmsi),ship_name,vessel_id_number,call_sign)
					cursor.execute(sql,values)
				else:
					sys.stderr.write("TODO: if ship exists, update its data if it changed\n")

		# Commit database changes
		db.commit()
	else:
		sys.stderr.write("No datatable found in website data\n")

else:
	sys.stderr.write(f"Error while fetching data:{r.status_code}\n")
