# Hospital-Datawarehouse

## Why snowflake?

Snowflake was chosen for its cloud-based, fully managed architecture, which eliminates the need for manual infrastructure management while providing high scalability and performance. Its unique separation of compute and storage allows independent scaling of resources, ensuring cost efficiency and faster query execution for analytical workloads. Snowflake supports structured and semi-structured data formats such as CSV, JSON, and XML, making it ideal for integrating diverse healthcare datasets from multiple sources. Additionally, features like Time Travel and automatic failover provide robust data integrity and recovery options, which are crucial for handling sensitive hospital data. Built-in security mechanisms, including encryption and role-based access control, ensure compliance with healthcare industry regulations and safeguard confidential information.

## Why python?

Python was primarily used for loading datasets into Snowflake, streamlining the data ingestion process. Using Python scripts, we automated the transfer of large healthcare datasets from local storage or cloud sources into Snowflake, reducing manual effort and potential errors. The integration was achieved using libraries like snowflake-connector-python, which facilitated seamless connection and data transfer. Python’s automation capabilities ensured efficient batch uploads, making it easier to handle large volumes of hospital data. This approach allowed for a structured and reliable ETL pipeline, ensuring that raw data was successfully ingested into Snowflake before undergoing further transformations within the data warehouse.

## Data Dictionary
## 1.⁠ ⁠Staging Tables

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

stg_measures

Stores unique healthcare measures used for quality and payment analysis.
	•	Measure_ID: A unique identifier for a specific healthcare measure.
	•	Measure_Name: A descriptive name for the measure.

stg_complications_facts

Stores data on hospital complications and deaths.
	•	Facility_ID: The facility associated with the measure.
	•	Measure_ID: The specific measure being evaluated.
	•	Start_Date: The date when data collection started.
	•	End_Date: The date when data collection ended.
	•	Score: The performance score for the measure.
	•	Lower_Estimate: The lower estimate of the measure’s confidence interval.
	•	Higher_Estimate: The higher estimate of the measure’s confidence interval.
	•	Denominator: The number of cases considered in the measure.
	•	Compared_to_National: How the facility’s performance compares to national benchmarks.

stg_infections_facts

Stores data on hospital-acquired infections.
	•	Facility_ID: The hospital associated with the infection measure.
	•	Measure_ID: The infection type being recorded.
	•	Start_Date: The date when data collection started.
	•	End_Date: The date when data collection ended.
	•	Score: The infection rate or score for the measure.
	•	Compared_to_National: Indicates if the infection rate is above, below, or the same as the national average

stg_payments_facts

Stores hospital payment and cost-related data.
	•	Facility_ID: The hospital associated with the payment record.
	•	Measure_ID: The measure representing a cost/payment category.
	•	Start_Date: The date when the payment data starts.
	•	End_Date: The date when the payment data ends.
	•	Payment: The average payment made for procedures.
	•	Lower_Estimate: The lower bound of the estimated payment range.
	•	Higher_Estimate: The upper bound of the estimated payment range.
	•	Denominator: The number of cases considered for this payment measure.
	•	Payment_Category: The category of the payment (e.g., inpatient, outpatient).

## 2.⁠ ⁠Normalized Tables

These tables store cleaned and structured data.

hospital_general_information

Holds detailed information about hospitals.
	•	Facility_ID: Unique identifier for a hospital.
	•	Facility_Name: Name of the hospital.
	•	Hospital_Type: The type of hospital.
	•	Hospital_Ownership: The entity that owns the hospital.
	•	Emergency_Services: Indicates whether emergency services are available.
	•	Hospital_Overall_Rating: The overall quality rating of the hospital.

complications_and_deaths

Stores data on hospital complications and mortality rates.
	•	Facility_ID: The hospital associated with the record.
	•	Measure_ID: The complication or death measure recorded.
	•	Denominator: The number of cases for this measure.
	•	Score: The performance score.
	•	Lower_Estimate: The lower bound of the measure’s confidence interval.
	•	Higher_Estimate: The upper bound of the measure’s confidence interval.
	•	Compared_to_National: Indicates if the score is better, worse, or the same as the national average.

payment_and_value_of_care

Stores financial and payment data.
	•	Facility_ID: The hospital linked to the payment record.
	•	Payment_Measure_ID: The identifier for the financial measure.
	•	Payment: The cost recorded for the procedure or service.
	•	Lower_Estimate: The lower estimate for the cost range.
	•	Higher_Estimate: The higher estimate for the cost range.

## 3.⁠ ⁠Dimensional Model (Star Schema)

The final structure optimized for analytics.

dim_facility

Stores detailed information about hospital facilities.
	•	Facility_SK: Surrogate key for a hospital.
	•	Facility_ID: The original hospital identifier.
	•	Facility_Name: Name of the hospital.
	•	Hospital_Type: Type of hospital (linked to dim_hospital_type).
	•	Hospital_Ownership: Ownership category (linked to dim_hospital_ownership).
	•	Emergency_Services: Whether emergency services are available.
	•	Overall_Rating: The hospital’s overall performance rating.

dim_hospital_type

Stores hospital type classifications.
	•	Type_SK: Surrogate key for hospital type.
	•	Hospital_Type: The type classification (e.g., General, Specialty).

dim_hospital_ownership

Stores hospital ownership details.
	•	Ownership_SK: Surrogate key for hospital ownership.
	•	Hospital_Ownership: The ownership type (e.g., Government, Private).

dim_measure

Stores all quality, infection, and payment measures.
	•	Measure_SK: Surrogate key for a healthcare measure.
	•	Measure_ID: Unique identifier for the measure.
	•	Measure_Name: Descriptive name of the measure.
	•	Measure_Category: Classification of the measure (e.g., Complication, Infection, Payment).

dim_date

Stores date-related information for tracking changes over time.
	•	Date_SK: Surrogate key for dates.
	•	Date_Key: Actual date value.
	•	Year: The year of the date.
	•	Month: The month of the date.
	•	Day: The day of the date.
	•	Day_of_Week: The numeric value representing the day of the week.
	•	Month_Name: The textual name of the month.
	•	Quarter: The quarter in which the date falls.
	•	Is_Weekend: Whether the date falls on a weekend.

## 4.⁠ ⁠Fact Tables

Fact tables store transactional data and connect to multiple dimensions.

fact_hospital_quality

Stores quality-related hospital performance data.
	•	Quality_Fact_SK: Surrogate key for quality fact records.
	•	Facility_SK: Foreign key referencing dim_facility.
	•	Measure_SK: Foreign key referencing dim_measure.
	•	Date_SK: Foreign key referencing dim_date.
	•	Score: The quality score of the measure.
	•	Lower_Estimate: The lower confidence interval for the measure.
	•	Higher_Estimate: The upper confidence interval for the measure.
	•	Denominator: The number of cases considered.
	•	Compared_to_National: Comparison of the hospital’s score with the national average.
	•	Fact_Type: Classification of the record (e.g., Complication, Infection).

fact_hospital_payment

Stores financial transactions and payment data for hospitals.
	•	Payment_Fact_SK: Surrogate key for the payment fact record.
	•	Facility_SK: Foreign key referencing dim_facility.
	•	Measure_SK: Foreign key referencing dim_measure.
	•	Date_SK: Foreign key referencing dim_date.
	•	Payment: The cost recorded for the service or procedure.
	•	Lower_Estimate: The lower bound of the estimated payment range.
	•	Higher_Estimate: The upper bound of the estimated payment range.
	•	Denominator: The number of cases considered in the payment analysis.
	•	Payment_Category: The type of payment measure recorded.