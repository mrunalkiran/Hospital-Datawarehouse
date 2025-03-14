# Hospital-Datawarehouse

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

