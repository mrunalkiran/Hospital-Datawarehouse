# Hospital-Datawarehouse
## Author: Mrunal Kiran
## Date: 2025-03-14
## Description: This script is used to create a data warehouse for hospital data.

## Data Dictionary
1.⁠ ⁠Staging Tables

Staging tables store raw data before transformation.

stg_facility

This table holds raw hospital facility information.
	•	Facility_ID: A unique identifier for each hospital or facility.
	•	Facility_Name: The official name of the hospital.
	•	Address: The street address of the hospital.
	•	City_Town: The city or town where the hospital is located.
	•	State: The state where the hospital operates.
	•	ZIP_Code: The ZIP code for the hospital’s location.
	•	County_Parish: The county or parish of the hospital.
	•	Telephone_Number: The contact number of the hospital.
	•	Hospital_Type: The classification of the hospital (e.g., General, Specialty).
	•	Hospital_Ownership: The entity that owns the hospital (e.g., Government, Private).
	•	Emergency_Services: Indicates whether the hospital provides emergency services.
	•	Meets_Criteria_for_Birthing_Friendly_Designation: Indicates if the hospital meets birthing-friendly criteria.
	•	Hospital_Overall_Rating: The overall rating of the hospital based on performance.