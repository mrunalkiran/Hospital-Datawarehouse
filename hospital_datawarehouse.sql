USE ROLE ACCOUNTADMIN;
SHOW WAREHOUSES;

ALTER WAREHOUSE COMPUTE_WH RESUME;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE hospital_dwh;
SHOW DATABASES;

USE DATABASE hospital_dwh;
CREATE STAGE raw.hospitalstage;
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA normalized;
CREATE SCHEMA dimensional;
show schemas;

SHOW STAGES IN SCHEMA RAW;
USE SCHEMA raw;

CREATE TABLE IF NOT EXISTS raw.raw_complications_and_deaths (
    Facility_ID string,
    Facility_Name string,
    Address string,
    City_Town string,
    State string,
    ZIP_Code string,
    County_Parish string,
    Telephone_Number string,
    Measure_ID string,
    Measure_Name string,
    Compared_to_National string,
    Denominator string,
    Score string,
    Lower_Estimate string,
    Higher_Estimate string,
    Footnote string,
    Start_Date string,
    End_Date string
);

CREATE TABLE IF NOT EXISTS raw.raw_hospital_general_information (
    Facility_ID string,
    Facility_Name string,
    Address string,
    City_Town string,
    State string,
    ZIP_Code string,
    County_Parish string,
    Telephone_Number string,
    Hospital_Type string,
    Hospital_Ownership string,
    Emergency_Services string,
    Meets_criteria_for_birthing_friendly_designation string,
    Hospital_Overall_Rating string,
    Hospital_Overall_Rating_Footnote string,
    MORT_Group_Measure_Count string,
    Count_of_Facility_MORT_Measures string,
    Count_of_MORT_Measures_Better string,
    Count_of_MORT_Measures_No_Different string,
    Count_of_MORT_Measures_Worse string,
    MORT_Group_Footnote string,
    Safety_Group_Measure_Count string,
    Count_of_Facility_Safety_Measures string,
    Count_of_Safety_Measures_Better string,
    Count_of_Safety_Measures_No_Different string,
    Count_of_Safety_Measures_Worse string,
    Safety_Group_Footnote string,
    READM_Group_Measure_Count string,
    Count_of_Facility_READM_Measures string,
    Count_of_READM_Measures_Better string,
    Count_of_READM_Measures_No_Different string,
    Count_of_READM_Measures_Worse string,
    READM_Group_Footnote string,
    Pt_Exp_Group_Measure_Count string,
    Count_of_Facility_Pt_Exp_Measures string,
    Pt_Exp_Group_Footnote string,
    TE_Group_Measure_Count string,
    Count_of_Facility_TE_Measures string,
    TE_Group_Footnote string
);

CREATE TABLE IF NOT EXISTS raw.raw_outpatient_imaging_efficiency (
    Facility_ID string,
    Facility_Name string,
    Address string,
    City_Town string,
    State string,
    ZIP_Code string,
    County_Parish string,
    Telephone_Number string,
    Measure_ID string,
    Measure_Name string,
    Score string,
    Footnote string,
    Start_Date string,
    End_Date string
);

CREATE TABLE IF NOT EXISTS raw.raw_payment_and_value_of_care (
    Facility_ID string,
    Facility_Name string,
    Address string,
    City_Town string,
    State string,
    ZIP_Code string,
    County_Parish string,
    Telephone_Number string,
    Payment_Measure_ID string,
    Payment_Measure_Name string,
    Payment_Category string,
    Denominator string,
    Payment string,
    Lower_Estimate string,
    Higher_Estimate string,
    Payment_Footnote string,
    Value_of_Care_Display_ID string,
    Value_of_Care_Display_Name string,
    Value_of_Care_Category string,
    Value_of_Care_Footnote string,
    Start_Date string,
    End_Date string
);

CREATE OR REPLACE TABLE raw.raw_healthcare_associated_infections (
    Facility_ID STRING,
    Facility_Name STRING,
    Address STRING,
    City_Town STRING,
    State STRING,
    ZIP_Code STRING,
    County_Parish STRING,
    Telephone_Number STRING,
    Measure_ID STRING,
    Measure_Name STRING,
    Compared_to_National STRING,  -- Stores "Not Available" as is
    Score STRING,  -- Changed to STRING to keep "Not Available" values
    Footnote STRING,  -- Changed to STRING to preserve raw data
    Start_Date STRING,
    End_Date STRING
);

CREATE OR REPLACE TABLE raw.raw_timely_and_effective_care (
    Facility_ID STRING,
    Facility_Name STRING,
    Address STRING,
    City_Town STRING,
    State STRING,
    ZIP_Code STRING,
    County_Parish STRING,
    Telephone_Number STRING,
    Measure_ID STRING,
    Measure_Name STRING,
    Compared_to_National STRING,
    Score STRING,  -- Stores "Not Available" as is
    "Sample" STRING,  -- Stores "Not Available" as is
    Footnote STRING,
    Start_Date STRING,
    End_Date STRING
);

COPY INTO raw.raw_complications_and_deaths
FROM @RAW.HOSPITALSTAGE/Complications_and_Deaths-Hospital.csv.gz
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',  -- ✅ Your file is tab-separated
    SKIP_HEADER = 1,  -- ✅ Skip header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',  -- ✅ Handle quoted text fields
    ESCAPE = '\\',  -- ✅ Escape special characters
    TRIM_SPACE = TRUE,  -- ✅ Remove extra spaces
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- ✅ Prevent row rejections due to column count mismatch
)
ON_ERROR = 'CONTINUE';
SELECT * FROM raw.raw_complications_and_deaths LIMIT 10;

LIST @RAW.HOSPITALSTAGE;

COPY INTO raw.raw_hospital_general_information
FROM @RAW.HOSPITALSTAGE/Hospital_General_Information.csv.gz
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',  -- ✅ Change to comma since your file uses ','
    SKIP_HEADER = 1,  -- ✅ Skip the header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',  -- ✅ Handles quoted text fields
    ESCAPE = '\\',  -- ✅ Escape special characters
    TRIM_SPACE = TRUE,  -- ✅ Removes extra spaces
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- ✅ Prevents row rejection due to column mismatch
)
ON_ERROR = 'CONTINUE';
SELECT * FROM raw.raw_hospital_general_information LIMIT 10;

COPY INTO raw.raw_outpatient_imaging_efficiency
FROM @RAW.HOSPITALSTAGE/Outpatient_Imaging_Efficiency-Hospital.csv.gz
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',  -- ✅ Change if needed (tab: '\t', pipe: '|')
    SKIP_HEADER = 1,  -- ✅ Skip the header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',  -- ✅ Handles quoted text fields
    ESCAPE = '\\',  -- ✅ Escape special characters
    TRIM_SPACE = TRUE,  -- ✅ Removes extra spaces
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- ✅ Prevents row rejection due to column mismatch
)
ON_ERROR = 'CONTINUE';

SELECT * FROM raw.raw_outpatient_imaging_efficiency LIMIT 10;

COPY INTO raw.raw_payment_and_value_of_care
FROM @RAW.HOSPITALSTAGE/Payment_and_Value_of_Care-Hospital.csv.gz
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',  -- ✅ Change if needed (tab: '\t', pipe: '|')
    SKIP_HEADER = 1,  -- ✅ Skip the header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',  -- ✅ Handles quoted text fields
    ESCAPE = '\\',  -- ✅ Escape special characters
    TRIM_SPACE = TRUE,  -- ✅ Removes extra spaces
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- ✅ Prevents row rejection due to column mismatch
)
ON_ERROR = 'CONTINUE';
SELECT * FROM raw.raw_payment_and_value_of_care LIMIT 10;

COPY INTO raw.raw_healthcare_associated_infections
FROM @RAW.HOSPITALSTAGE/Healthcare_Associated_Infections-Hospital.csv.gz
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',  -- ✅ Change if needed (tab: '\t', pipe: '|')
    SKIP_HEADER = 1,  -- ✅ Skip the header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',  -- ✅ Handles quoted text fields
    ESCAPE = '\\',  -- ✅ Escape special characters
    TRIM_SPACE = TRUE,  -- ✅ Removes extra spaces
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- ✅ Prevents row rejection due to column mismatch
)
ON_ERROR = 'CONTINUE';

COPY INTO raw.raw_timely_and_effective_care
FROM @RAW.HOSPITALSTAGE/Timely_and_Effective_Care-Hospital.csv.gz
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',  -- ✅ Change if needed (tab: '\t', pipe: '|')
    SKIP_HEADER = 1,  -- ✅ Skip the header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',  -- ✅ Handles quoted text fields
    ESCAPE = '\\',  -- ✅ Escape special characters
    TRIM_SPACE = TRUE,  -- ✅ Removes extra spaces
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- ✅ Prevents row rejection due to column mismatch
)
ON_ERROR = 'CONTINUE';

USE SCHEMA normalized;
CREATE TABLE IF NOT EXISTS normalized.complications_and_deaths (
    Facility_ID string,
    Facility_Name string,
    Address string,
    City_Town string,
    State string,
    ZIP_Code string,
    County_Parish string,
    Telephone_Number string,
    Measure_ID string,
    Measure_Name string,
    Compared_to_National string,
    Denominator int,
    Score float,
    Lower_Estimate float,
    Higher_Estimate float,
    Footnote string,
    Start_Date string,
    End_Date string
);

CREATE TABLE IF NOT EXISTS normalized.hospital_general_information (
    Facility_ID string,
    Facility_Name string,
    Address string,
    City_Town string,
    State string,
    ZIP_Code string,
    County_Parish string,
    Telephone_Number string,
    Hospital_Type string,
    Hospital_Ownership string,
    Emergency_Services string,
    Meets_criteria_for_birthing_friendly_designation string,
    Hospital_Overall_Rating string,
    Hospital_Overall_Rating_Footnote string,
    MORT_Group_Measure_Count int,
    Count_of_Facility_MORT_Measures int,
    Count_of_MORT_Measures_Better int,
    Count_of_MORT_Measures_No_Different int,
    Count_of_MORT_Measures_Worse int,
    MORT_Group_Footnote string,
    Safety_Group_Measure_Count int,
    Count_of_Facility_Safety_Measures int,
    Count_of_Safety_Measures_Better int,
    Count_of_Safety_Measures_No_Different int,
    Count_of_Safety_Measures_Worse int,
    Safety_Group_Footnote string,
    READM_Group_Measure_Count int,
    Count_of_Facility_READM_Measures int,
    Count_of_READM_Measures_Better int,
    Count_of_READM_Measures_No_Different int,
    Count_of_READM_Measures_Worse int,
    READM_Group_Footnote string,
    Pt_Exp_Group_Measure_Count int,
    Count_of_Facility_Pt_Exp_Measures int,
    Pt_Exp_Group_Footnote string,
    TE_Group_Measure_Count int,
    Count_of_Facility_TE_Measures int,
    TE_Group_Footnote string
);

CREATE TABLE IF NOT EXISTS normalized.outpatient_imaging_efficiency (
    Facility_ID string,
    Facility_Name string,
    Address string,
    City_Town string,
    State string,
    ZIP_Code string,
    County_Parish string,
    Telephone_Number string,
    Measure_ID string,
    Measure_Name string,
    Score float,
    Footnote string,
    Start_Date string,
    End_Date string
);

CREATE TABLE IF NOT EXISTS normalized.payment_and_value_of_care (
    Facility_ID string,
    Facility_Name string,
    Address string,
    City_Town string,
    State string,
    ZIP_Code string,
    County_Parish string,
    Telephone_Number string,
    Payment_Measure_ID string,
    Payment_Measure_Name string,
    Payment_Category string,
    Denominator int,
    Payment float,
    Lower_Estimate float,
    Higher_Estimate float,
    Payment_Footnote string,
    Value_of_Care_Display_ID string,
    Value_of_Care_Display_Name string,
    Value_of_Care_Category string,
    Value_of_Care_Footnote string,
    Start_Date string,
    End_Date string
);

CREATE TABLE IF NOT EXISTS normalized.healthcare_associated_infections (
    Facility_ID string,
    Facility_Name string,
    Address string,
    City_Town string,
    State string,
    ZIP_Code string,
    County_Parish string,
    Telephone_Number string,
    Measure_ID string,
    Measure_Name string,
    Compared_to_National string,
    Score float,
    Footnote string,
    Start_Date string,
    End_Date string
);

CREATE TABLE IF NOT EXISTS normalized.timely_and_effective_care (
    Facility_ID string,
    Facility_Name string,
    Address string,
    City_Town string,
    State string,
    ZIP_Code string,
    County_Parish string,
    Telephone_Number string,
    Measure_ID string,
    Measure_Name string,
    Compared_to_National string,
    Score float,
    "Sample" string,
    Footnote string,
    Start_Date string,
    End_Date string
);

MERGE INTO normalized.complications_and_deaths AS target
USING (
    SELECT 
        Facility_ID,
        NULLIF(TRIM(Facility_Name), '') AS Facility_Name,
        NULLIF(TRIM(Address), '') AS Address,
        NULLIF(TRIM(City_Town), '') AS City_Town,
        NULLIF(TRIM(State), '') AS State,
        NULLIF(TRIM(ZIP_Code), '') AS ZIP_Code,
        NULLIF(TRIM(County_Parish), '') AS County_Parish,
        NULLIF(TRIM(Telephone_Number), '') AS Telephone_Number,
        NULLIF(TRIM(Measure_ID), '') AS Measure_ID,
        NULLIF(TRIM(Measure_Name), '') AS Measure_Name,
        NULLIF(TRIM(Compared_to_National), '') AS Compared_to_National,
        COALESCE(TRY_CAST(NULLIF(TRIM(Denominator), 'Not Available') AS INT), 0) AS Denominator,
        COALESCE(TRY_CAST(NULLIF(TRIM(Score), 'Not Available') AS FLOAT), 0.0) AS Score,
        COALESCE(TRY_CAST(NULLIF(TRIM(Lower_Estimate), 'Not Available') AS FLOAT), 0.0) AS Lower_Estimate,
        COALESCE(TRY_CAST(NULLIF(TRIM(Higher_Estimate), 'Not Available') AS FLOAT), 0.0) AS Higher_Estimate,
        NULLIF(TRIM(Footnote), '') AS Footnote,
        TRY_TO_DATE(NULLIF(TRIM(Start_Date), ''), 'YYYY-MM-DD') AS Start_Date,
        TRY_TO_DATE(NULLIF(TRIM(End_Date), ''), 'YYYY-MM-DD') AS End_Date
    FROM raw.raw_complications_and_deaths
    WHERE Facility_ID IS NOT NULL
) AS source
ON target.Facility_ID = source.Facility_ID 
AND target.Measure_ID = source.Measure_ID  -- Ensures updates happen for the same measure
WHEN MATCHED THEN
    UPDATE SET
        target.Score = source.Score,
        target.Lower_Estimate = source.Lower_Estimate,
        target.Higher_Estimate = source.Higher_Estimate,
        target.Denominator = source.Denominator
WHEN NOT MATCHED THEN
    INSERT (
        Facility_ID, Facility_Name, Address, City_Town, State, ZIP_Code, 
        County_Parish, Telephone_Number, Measure_ID, Measure_Name, Compared_to_National, 
        Denominator, Score, Lower_Estimate, Higher_Estimate, Footnote, Start_Date, End_Date
    )
    VALUES (
        source.Facility_ID, source.Facility_Name, source.Address, source.City_Town, 
        source.State, source.ZIP_Code, source.County_Parish, source.Telephone_Number, 
        source.Measure_ID, source.Measure_Name, source.Compared_to_National, 
        source.Denominator, source.Score, source.Lower_Estimate, source.Higher_Estimate, 
        source.Footnote, source.Start_Date, source.End_Date
    );

MERGE INTO normalized.hospital_general_information AS target
USING (
    SELECT DISTINCT 
        Facility_ID,
        NULLIF(TRIM(Facility_Name), '') AS Facility_Name,
        NULLIF(TRIM(Address), '') AS Address,
        NULLIF(TRIM(City_Town), '') AS City_Town,
        NULLIF(TRIM(State), '') AS State,
        NULLIF(TRIM(ZIP_Code), '') AS ZIP_Code,
        NULLIF(TRIM(County_Parish), '') AS County_Parish,
        NULLIF(TRIM(Telephone_Number), '') AS Telephone_Number,
        NULLIF(TRIM(Hospital_Type), '') AS Hospital_Type,
        NULLIF(TRIM(Hospital_Ownership), '') AS Hospital_Ownership,
        NULLIF(TRIM(Emergency_Services), '') AS Emergency_Services,
        NULLIF(TRIM(Meets_criteria_for_birthing_friendly_designation), '') AS Meets_criteria_for_birthing_friendly_designation,
        NULLIF(TRIM(Hospital_Overall_Rating), '') AS Hospital_Overall_Rating,
        NULLIF(TRIM(Hospital_Overall_Rating_Footnote), '') AS Hospital_Overall_Rating_Footnote,
        COALESCE(TRY_CAST(NULLIF(TRIM(MORT_Group_Measure_Count), 'Not Available') AS INT), 0) AS MORT_Group_Measure_Count,
        COALESCE(TRY_CAST(NULLIF(TRIM(Safety_Group_Measure_Count), 'Not Available') AS INT), 0) AS Safety_Group_Measure_Count,
        COALESCE(TRY_CAST(NULLIF(TRIM(READM_Group_Measure_Count), 'Not Available') AS INT), 0) AS READM_Group_Measure_Count,
        COALESCE(TRY_CAST(NULLIF(TRIM(Pt_Exp_Group_Measure_Count), 'Not Available') AS INT), 0) AS Pt_Exp_Group_Measure_Count,
        COALESCE(TRY_CAST(NULLIF(TRIM(TE_Group_Measure_Count), 'Not Available') AS INT), 0) AS TE_Group_Measure_Count
    FROM raw.raw_hospital_general_information
    WHERE Facility_ID IS NOT NULL
) AS source
ON target.Facility_ID = source.Facility_ID
WHEN MATCHED THEN
    UPDATE SET
        target.Hospital_Overall_Rating = source.Hospital_Overall_Rating
WHEN NOT MATCHED THEN
    INSERT (
        Facility_ID, Facility_Name, Address, City_Town, State, ZIP_Code, 
        County_Parish, Telephone_Number, Hospital_Type, Hospital_Ownership, 
        Emergency_Services, Meets_criteria_for_birthing_friendly_designation, 
        Hospital_Overall_Rating, Hospital_Overall_Rating_Footnote, 
        MORT_Group_Measure_Count, Safety_Group_Measure_Count, READM_Group_Measure_Count, 
        Pt_Exp_Group_Measure_Count, TE_Group_Measure_Count
    )
    VALUES (
        source.Facility_ID, source.Facility_Name, source.Address, source.City_Town, 
        source.State, source.ZIP_Code, source.County_Parish, source.Telephone_Number, 
        source.Hospital_Type, source.Hospital_Ownership, source.Emergency_Services, 
        source.Meets_criteria_for_birthing_friendly_designation, source.Hospital_Overall_Rating, 
        source.Hospital_Overall_Rating_Footnote, source.MORT_Group_Measure_Count, 
        source.Safety_Group_Measure_Count, source.READM_Group_Measure_Count, 
        source.Pt_Exp_Group_Measure_Count, source.TE_Group_Measure_Count
    );

MERGE INTO normalized.hospital_general_information AS target
USING (
    SELECT DISTINCT 
        Facility_ID,
        TRIM(Facility_Name) AS Facility_Name,
        TRIM(Address) AS Address,
        TRIM(City_Town) AS City_Town,
        TRIM(State) AS State,
        TRIM(ZIP_Code) AS ZIP_Code,
        TRIM(County_Parish) AS County_Parish,
        TRIM(Telephone_Number) AS Telephone_Number,
        TRIM(Hospital_Type) AS Hospital_Type,
        TRIM(Hospital_Ownership) AS Hospital_Ownership,
        TRIM(Emergency_Services) AS Emergency_Services,
        TRIM(Meets_criteria_for_birthing_friendly_designation) AS Meets_criteria_for_birthing_friendly_designation,
        TRIM(Hospital_Overall_Rating) AS Hospital_Overall_Rating,
        TRIM(Hospital_Overall_Rating_Footnote) AS Hospital_Overall_Rating_Footnote,
        TRY_CAST(NULLIF(MORT_Group_Measure_Count, 'Not Available') AS INT) AS MORT_Group_Measure_Count,
        TRY_CAST(NULLIF(Safety_Group_Measure_Count, 'Not Available') AS INT) AS Safety_Group_Measure_Count,
        TRY_CAST(NULLIF(READM_Group_Measure_Count, 'Not Available') AS INT) AS READM_Group_Measure_Count
    FROM raw.raw_hospital_general_information
    WHERE Facility_ID IS NOT NULL
) AS source
ON target.Facility_ID = source.Facility_ID
WHEN MATCHED THEN
    UPDATE SET target.Hospital_Overall_Rating = source.Hospital_Overall_Rating
WHEN NOT MATCHED THEN
    INSERT (
        Facility_ID, Facility_Name, Address, City_Town, State, ZIP_Code, 
        County_Parish, Telephone_Number, Hospital_Type, Hospital_Ownership, 
        Emergency_Services, Meets_criteria_for_birthing_friendly_designation, 
        Hospital_Overall_Rating, Hospital_Overall_Rating_Footnote, 
        MORT_Group_Measure_Count, Safety_Group_Measure_Count, READM_Group_Measure_Count
    )
    VALUES (
        source.Facility_ID, source.Facility_Name, source.Address, source.City_Town, 
        source.State, source.ZIP_Code, source.County_Parish, source.Telephone_Number, 
        source.Hospital_Type, source.Hospital_Ownership, source.Emergency_Services, 
        source.Meets_criteria_for_birthing_friendly_designation, source.Hospital_Overall_Rating, 
        source.Hospital_Overall_Rating_Footnote, source.MORT_Group_Measure_Count, 
        source.Safety_Group_Measure_Count, source.READM_Group_Measure_Count
    );

MERGE INTO normalized.outpatient_imaging_efficiency AS target
USING (
    SELECT DISTINCT
        Facility_ID,
        TRIM(Facility_Name) AS Facility_Name,
        TRIM(Address) AS Address,
        TRIM(City_Town) AS City_Town,
        TRIM(State) AS State,
        TRIM(ZIP_Code) AS ZIP_Code,
        TRIM(County_Parish) AS County_Parish,
        TRIM(Telephone_Number) AS Telephone_Number,
        TRIM(Measure_ID) AS Measure_ID,
        TRIM(Measure_Name) AS Measure_Name,
        TRY_CAST(NULLIF(TRIM(Score), 'Not Available') AS FLOAT) AS Score,
        TRIM(Footnote) AS Footnote,
        TRY_TO_DATE(TRIM(Start_Date), 'YYYY-MM-DD') AS Start_Date,
        TRY_TO_DATE(TRIM(End_Date), 'YYYY-MM-DD') AS End_Date
    FROM raw.raw_outpatient_imaging_efficiency
    WHERE Facility_ID IS NOT NULL
) AS source
ON target.Facility_ID = source.Facility_ID
AND target.Measure_ID = source.Measure_ID  -- Ensures updates apply to the same measure
WHEN MATCHED THEN
    UPDATE SET
        target.Score = source.Score,
        target.Footnote = source.Footnote,
        target.Start_Date = source.Start_Date,
        target.End_Date = source.End_Date
WHEN NOT MATCHED THEN
    INSERT (
        Facility_ID, Facility_Name, Address, City_Town, State, ZIP_Code, 
        County_Parish, Telephone_Number, Measure_ID, Measure_Name, Score, 
        Footnote, Start_Date, End_Date
    )
    VALUES (
        source.Facility_ID, source.Facility_Name, source.Address, source.City_Town, 
        source.State, source.ZIP_Code, source.County_Parish, source.Telephone_Number, 
        source.Measure_ID, source.Measure_Name, source.Score, source.Footnote, 
        source.Start_Date, source.End_Date
    );

MERGE INTO normalized.payment_and_value_of_care AS target
USING (
    SELECT DISTINCT
        Facility_ID,
        TRIM(Facility_Name) AS Facility_Name,
        TRIM(Address) AS Address,
        TRIM(City_Town) AS City_Town,
        TRIM(State) AS State,
        TRIM(ZIP_Code) AS ZIP_Code,
        TRIM(County_Parish) AS County_Parish,
        TRIM(Telephone_Number) AS Telephone_Number,
        TRIM(Payment_Measure_ID) AS Payment_Measure_ID,
        TRIM(Payment_Measure_Name) AS Payment_Measure_Name,
        TRIM(Payment_Category) AS Payment_Category,
        TRY_CAST(NULLIF(Denominator, 'Not Available') AS INT) AS Denominator,
        TRY_CAST(NULLIF(Payment, 'Not Available') AS FLOAT) AS Payment,
        TRY_CAST(NULLIF(Lower_Estimate, 'Not Available') AS FLOAT) AS Lower_Estimate,
        TRY_CAST(NULLIF(Higher_Estimate, 'Not Available') AS FLOAT) AS Higher_Estimate,
        TRIM(Payment_Footnote) AS Payment_Footnote,
        TRIM(Value_of_Care_Display_ID) AS Value_of_Care_Display_ID,
        TRIM(Value_of_Care_Display_Name) AS Value_of_Care_Display_Name,
        TRIM(Value_of_Care_Category) AS Value_of_Care_Category,
        TRIM(Value_of_Care_Footnote) AS Value_of_Care_Footnote,
        TRY_TO_DATE(TRIM(Start_Date), 'YYYY-MM-DD') AS Start_Date,
        TRY_TO_DATE(TRIM(End_Date), 'YYYY-MM-DD') AS End_Date
    FROM raw.raw_payment_and_value_of_care
    WHERE Facility_ID IS NOT NULL
) AS source
ON target.Facility_ID = source.Facility_ID
AND target.Payment_Measure_ID = source.Payment_Measure_ID
WHEN MATCHED THEN
    UPDATE SET
        target.Payment = source.Payment,
        target.Lower_Estimate = source.Lower_Estimate,
        target.Higher_Estimate = source.Higher_Estimate,
        target.Denominator = source.Denominator
WHEN NOT MATCHED THEN
    INSERT (
        Facility_ID, Facility_Name, Address, City_Town, State, ZIP_Code, 
        County_Parish, Telephone_Number, Payment_Measure_ID, Payment_Measure_Name, 
        Payment_Category, Denominator, Payment, Lower_Estimate, Higher_Estimate, 
        Payment_Footnote, Value_of_Care_Display_ID, Value_of_Care_Display_Name, 
        Value_of_Care_Category, Value_of_Care_Footnote, Start_Date, End_Date
    )
    VALUES (
        source.Facility_ID, source.Facility_Name, source.Address, source.City_Town, 
        source.State, source.ZIP_Code, source.County_Parish, source.Telephone_Number, 
        source.Payment_Measure_ID, source.Payment_Measure_Name, source.Payment_Category, 
        source.Denominator, source.Payment, source.Lower_Estimate, source.Higher_Estimate, 
        source.Payment_Footnote, source.Value_of_Care_Display_ID, source.Value_of_Care_Display_Name, 
        source.Value_of_Care_Category, source.Value_of_Care_Footnote, source.Start_Date, 
        source.End_Date
    );

MERGE INTO normalized.healthcare_associated_infections AS target
USING (
    SELECT DISTINCT
        Facility_ID,
        TRIM(Facility_Name) AS Facility_Name,
        TRIM(Address) AS Address,
        TRIM(City_Town) AS City_Town,
        TRIM(State) AS State,
        TRIM(ZIP_Code) AS ZIP_Code,
        TRIM(County_Parish) AS County_Parish,
        TRIM(Telephone_Number) AS Telephone_Number,
        TRIM(Measure_ID) AS Measure_ID,
        TRIM(Measure_Name) AS Measure_Name,
        TRIM(Compared_to_National) AS Compared_to_National,
        TRY_CAST(NULLIF(Score, 'Not Available') AS FLOAT) AS Score,
        TRIM(Footnote) AS Footnote,
        TRY_TO_DATE(TRIM(Start_Date), 'YYYY-MM-DD') AS Start_Date,
        TRY_TO_DATE(TRIM(End_Date), 'YYYY-MM-DD') AS End_Date
    FROM raw.raw_healthcare_associated_infections
    WHERE Facility_ID IS NOT NULL
) AS source
ON target.Facility_ID = source.Facility_ID
AND target.Measure_ID = source.Measure_ID
WHEN MATCHED THEN
    UPDATE SET
        target.Score = source.Score,
        target.Footnote = source.Footnote,
        target.Start_Date = source.Start_Date,
        target.End_Date = source.End_Date
WHEN NOT MATCHED THEN
    INSERT (
        Facility_ID, Facility_Name, Address, City_Town, State, ZIP_Code, 
        County_Parish, Telephone_Number, Measure_ID, Measure_Name, Compared_to_National, 
        Score, Footnote, Start_Date, End_Date
    )
    VALUES (
        source.Facility_ID, source.Facility_Name, source.Address, source.City_Town, 
        source.State, source.ZIP_Code, source.County_Parish, source.Telephone_Number, 
        source.Measure_ID, source.Measure_Name, source.Compared_to_National, source.Score, 
        source.Footnote, source.Start_Date, source.End_Date
    );

MERGE INTO normalized.timely_and_effective_care AS target
USING (
    SELECT DISTINCT
        Facility_ID,
        TRIM(Facility_Name) AS Facility_Name,
        TRIM(Address) AS Address,
        TRIM(City_Town) AS City_Town,
        TRIM(State) AS State,
        TRIM(ZIP_Code) AS ZIP_Code,
        TRIM(County_Parish) AS County_Parish,
        TRIM(Telephone_Number) AS Telephone_Number,
        TRIM(Measure_ID) AS Measure_ID,
        TRIM(Measure_Name) AS Measure_Name,
        TRIM(Compared_to_National) AS Compared_to_National,
        TRY_CAST(NULLIF(Score, 'Not Available') AS FLOAT) AS Score,
        TRIM("Sample") AS "Sample",
        TRIM(Footnote) AS Footnote,
        TRY_TO_DATE(TRIM(Start_Date), 'YYYY-MM-DD') AS Start_Date,
        TRY_TO_DATE(TRIM(End_Date), 'YYYY-MM-DD') AS End_Date
    FROM raw.raw_timely_and_effective_care
    WHERE Facility_ID IS NOT NULL
) AS source
ON target.Facility_ID = source.Facility_ID
AND target.Measure_ID = source.Measure_ID
WHEN MATCHED THEN
    UPDATE SET 
        target.Score = source.Score,
        target."Sample" = source."Sample",
        target.Footnote = source.Footnote,
        target.Start_Date = source.Start_Date,
        target.End_Date = source.End_Date
WHEN NOT MATCHED THEN
    INSERT (
        Facility_ID, Facility_Name, Address, City_Town, State, ZIP_Code, 
        County_Parish, Telephone_Number, Measure_ID, Measure_Name, Compared_to_National, 
        Score, "Sample", Footnote, Start_Date, End_Date
    )
    VALUES (
        source.Facility_ID, source.Facility_Name, source.Address, source.City_Town, 
        source.State, source.ZIP_Code, source.County_Parish, source.Telephone_Number, 
        source.Measure_ID, source.Measure_Name, source.Compared_to_National, source.Score, 
        source."Sample", source.Footnote, source.Start_Date, source.End_Date
    );
    
SELECT * FROM normalized.complications_and_deaths LIMIT 10;
SELECT * FROM normalized.hospital_general_information LIMIT 10;
SELECT * FROM normalized.outpatient_imaging_efficiency LIMIT 10;
SELECT * FROM normalized.payment_and_value_of_care LIMIT 10;
SELECT * FROM normalized.healthcare_associated_infections LIMIT 10;
SELECT * FROM normalized.timely_and_effective_care LIMIT 10;    

USE SCHEMA STAGING;

-- Create a staging table for facility dimension
CREATE OR REPLACE TABLE staging.stg_facility AS
SELECT DISTINCT
    Facility_ID,
    Facility_Name,
    Address,
    City_Town,
    State,
    ZIP_Code,
    County_Parish,
    Telephone_Number,
    Hospital_Type,
    Hospital_Ownership,
    Emergency_Services,
    Meets_criteria_for_birthing_friendly_designation,
    Hospital_Overall_Rating
FROM normalized.hospital_general_information;

SELECT * FROM staging.stg_facility LIMIT 10;

-- Create a staging table for measures
CREATE OR REPLACE TABLE staging.stg_measures AS
SELECT DISTINCT
    Measure_ID,
    Measure_Name
FROM (
    SELECT Measure_ID, Measure_Name FROM normalized.complications_and_deaths
    UNION
    SELECT Measure_ID, Measure_Name FROM normalized.outpatient_imaging_efficiency
    UNION
    SELECT Measure_ID, Measure_Name FROM normalized.healthcare_associated_infections
    UNION
    SELECT Measure_ID, Measure_Name FROM normalized.timely_and_effective_care
    UNION
    SELECT Payment_Measure_ID AS Measure_ID, Payment_Measure_Name AS Measure_Name FROM normalized.payment_and_value_of_care
)
WHERE Measure_ID IS NOT NULL;

SELECT * FROM staging.stg_measures LIMIT 10;

-- Create a staging table for date dimension
CREATE OR REPLACE TABLE staging.stg_date AS
SELECT DISTINCT
    CAST(Date_Value AS DATE) AS Date_Key,
    YEAR(CAST(Date_Value AS DATE)) AS Year,
    MONTH(CAST(Date_Value AS DATE)) AS Month,
    DAY(CAST(Date_Value AS DATE)) AS Day,
    DAYOFWEEK(CAST(Date_Value AS DATE)) AS Day_Of_Week,
    MONTHNAME(CAST(Date_Value AS DATE)) AS Month_Name,
    QUARTER(CAST(Date_Value AS DATE)) AS Quarter
FROM (
    SELECT Start_Date AS Date_Value FROM normalized.complications_and_deaths WHERE Start_Date IS NOT NULL
    UNION
    SELECT End_Date AS Date_Value FROM normalized.complications_and_deaths WHERE End_Date IS NOT NULL
    UNION
    SELECT Start_Date AS Date_Value FROM normalized.outpatient_imaging_efficiency WHERE Start_Date IS NOT NULL
    UNION
    SELECT End_Date AS Date_Value FROM normalized.outpatient_imaging_efficiency WHERE End_Date IS NOT NULL
    UNION
    SELECT Start_Date AS Date_Value FROM normalized.healthcare_associated_infections WHERE Start_Date IS NOT NULL
    UNION
    SELECT End_Date AS Date_Value FROM normalized.healthcare_associated_infections WHERE End_Date IS NOT NULL
    UNION
    SELECT Start_Date AS Date_Value FROM normalized.timely_and_effective_care WHERE Start_Date IS NOT NULL
    UNION
    SELECT End_Date AS Date_Value FROM normalized.timely_and_effective_care WHERE End_Date IS NOT NULL
    UNION
    SELECT Start_Date AS Date_Value FROM normalized.payment_and_value_of_care WHERE Start_Date IS NOT NULL
    UNION
    SELECT End_Date AS Date_Value FROM normalized.payment_and_value_of_care WHERE End_Date IS NOT NULL
);

SELECT * FROM staging.stg_date LIMIT 10;

-- Create staging table for complications fact data
CREATE OR REPLACE TABLE staging.stg_complications_facts AS
SELECT
    Facility_ID,
    Measure_ID,
    Start_Date,
    End_Date,
    Score,
    Lower_Estimate,
    Higher_Estimate,
    Denominator,
    Compared_to_National
FROM normalized.complications_and_deaths;

SELECT * FROM staging.stg_complications_facts LIMIT 10;

-- ⁠Create staging table for infections fact data
CREATE OR REPLACE TABLE staging.stg_infections_facts AS
SELECT
    Facility_ID,
    Measure_ID,
    Start_Date,
    End_Date,
    Score,
    Compared_to_National
FROM normalized.healthcare_associated_infections;

SELECT * FROM staging.stg_infections_facts LIMIT 10;

-- Create staging table for payments fact data
CREATE OR REPLACE TABLE staging.stg_payments_facts AS
SELECT
    Facility_ID,
    Payment_Measure_ID AS Measure_ID,
    Start_Date,
    End_Date,
    Payment AS Score,
    Lower_Estimate,
    Higher_Estimate,
    Denominator,
    Payment_Category
FROM normalized.payment_and_value_of_care;

SELECT * FROM staging.stg_payments_facts LIMIT 10;

USE SCHEMA dimensional;

-- Create Facility Dimension Table with Correct Date Default
CREATE OR REPLACE TABLE dimensional.dim_facility (
    Facility_SK INT AUTOINCREMENT PRIMARY KEY,
    Facility_ID STRING NOT NULL UNIQUE,
    Facility_Name STRING,
    Address STRING,
    City_Town STRING,
    State STRING,
    ZIP_Code STRING,
    County_Parish STRING,
    Telephone_Number STRING,
    Hospital_Type STRING,
    Hospital_Ownership STRING,
    Emergency_Services STRING,
    Birthing_Friendly STRING,
    Overall_Rating STRING,
    Valid_From DATE DEFAULT CURRENT_DATE(),
    Valid_To DATE DEFAULT DATE '9999-12-31', -- ✅ Fix: Use DATE keyword
    Is_Current BOOLEAN DEFAULT TRUE
);

INSERT INTO dimensional.dim_facility (
    Facility_ID, Facility_Name, Address, City_Town, State, ZIP_Code, 
    County_Parish, Telephone_Number, Hospital_Type, Hospital_Ownership, 
    Emergency_Services, Birthing_Friendly, Overall_Rating
)
SELECT DISTINCT
    Facility_ID,
    Facility_Name,
    Address,
    City_Town,
    State,
    ZIP_Code,
    County_Parish,
    Telephone_Number,
    Hospital_Type,
    Hospital_Ownership,
    Emergency_Services,
    Meets_criteria_for_birthing_friendly_designation AS Birthing_Friendly,
    Hospital_Overall_Rating AS Overall_Rating
FROM staging.stg_facility
WHERE Facility_ID IS NOT NULL;

SELECT * FROM dimensional.dim_facility LIMIT 10;

-- Create Measure Dimension Table
CREATE OR REPLACE TABLE dimensional.dim_measure (
    Measure_SK INT AUTOINCREMENT PRIMARY KEY,
    Measure_ID STRING NOT NULL UNIQUE,
    Measure_Name STRING,
    Measure_Category STRING,
    Valid_From DATE DEFAULT CURRENT_DATE(),
    Valid_To DATE DEFAULT DATE  '9999-12-31',
    Is_Current BOOLEAN DEFAULT TRUE
);

INSERT INTO dimensional.dim_measure (
    Measure_ID, Measure_Name, Measure_Category
)
SELECT DISTINCT
    m.Measure_ID,
    m.Measure_Name,
    CASE 
        WHEN c.Measure_ID IS NOT NULL THEN 'Complications and Deaths'
        WHEN i.Measure_ID IS NOT NULL THEN 'Healthcare Associated Infections'
        WHEN t.Measure_ID IS NOT NULL THEN 'Timely and Effective Care'
        WHEN o.Measure_ID IS NOT NULL THEN 'Outpatient Imaging Efficiency'
        WHEN p.Payment_Measure_ID IS NOT NULL THEN 'Payment and Value of Care'
        ELSE 'Other'
    END AS Measure_Category
FROM staging.stg_measures m
LEFT JOIN normalized.complications_and_deaths c ON m.Measure_ID = c.Measure_ID
LEFT JOIN normalized.healthcare_associated_infections i ON m.Measure_ID = i.Measure_ID
LEFT JOIN normalized.timely_and_effective_care t ON m.Measure_ID = t.Measure_ID
LEFT JOIN normalized.outpatient_imaging_efficiency o ON m.Measure_ID = o.Measure_ID
LEFT JOIN normalized.payment_and_value_of_care p ON m.Measure_ID = p.Payment_Measure_ID
WHERE m.Measure_ID IS NOT NULL;

SELECT * FROM dimensional.dim_measure LIMIT 10;

-- Create Date Dimension Table
CREATE OR REPLACE TABLE dimensional.dim_date (
    Date_SK INT AUTOINCREMENT PRIMARY KEY,
    Date_Key DATE NOT NULL UNIQUE,
    Year INT,
    Month INT,
    Day INT,
    Day_of_Week INT,
    Month_Name STRING,
    Quarter INT,
    Is_Weekend BOOLEAN
);

-- Load Data into dim_date
INSERT INTO dimensional.dim_date (
    Date_Key, Year, Month, Day, Day_of_Week, Month_Name, Quarter, Is_Weekend
)
SELECT DISTINCT
    Date_Key,
    Year,
    Month,
    Day,
    Day_Of_Week,
    Month_Name,
    Quarter,
    CASE WHEN Day_Of_Week IN (0, 6) THEN TRUE ELSE FALSE END AS Is_Weekend
FROM staging.stg_date;

SELECT * FROM dimensional.dim_date LIMIT 10;

CREATE OR REPLACE TABLE dimensional.dim_hospital_type (
    Type_SK INT AUTOINCREMENT PRIMARY KEY,
    Hospital_Type STRING NOT NULL UNIQUE
);

INSERT INTO dimensional.dim_hospital_type (Hospital_Type)
SELECT DISTINCT Hospital_Type FROM staging.stg_facility;

SELECT * FROM dimensional.dim_hospital_type LIMIT 10;

CREATE OR REPLACE TABLE dimensional.dim_hospital_ownership (
    Ownership_SK INT AUTOINCREMENT PRIMARY KEY,
    Hospital_Ownership STRING NOT NULL UNIQUE
);

INSERT INTO dimensional.dim_hospital_ownership (Hospital_Ownership)
SELECT DISTINCT Hospital_Ownership FROM staging.stg_facility;

SELECT * FROM dimensional.dim_hospital_ownership LIMIT 10;

CREATE OR REPLACE TABLE dimensional.fact_hospital_quality (
    Quality_Fact_SK INT AUTOINCREMENT PRIMARY KEY,
    Facility_SK INT NOT NULL,
    Measure_SK INT NOT NULL,
    Date_SK INT NOT NULL,
    Score FLOAT,
    Lower_Estimate FLOAT,
    Higher_Estimate FLOAT,
    Denominator INT,
    Compared_to_National STRING,
    Fact_Type STRING, -- 'Complication', 'Infection', etc.
    FOREIGN KEY (Facility_SK) REFERENCES dimensional.dim_facility(Facility_SK),
    FOREIGN KEY (Measure_SK) REFERENCES dimensional.dim_measure(Measure_SK),
    FOREIGN KEY (Date_SK) REFERENCES dimensional.dim_date(Date_SK)
);

INSERT INTO dimensional.fact_hospital_quality (
    Facility_SK, Measure_SK, Date_SK, Score, 
    Lower_Estimate, Higher_Estimate, Denominator, Compared_to_National, Fact_Type
)
SELECT 
    f.Facility_SK,
    m.Measure_SK,
    d.Date_SK,
    c.Score,
    c.Lower_Estimate,
    c.Higher_Estimate,
    c.Denominator,
    c.Compared_to_National,
    'Complication'
FROM staging.stg_complications_facts c
JOIN dimensional.dim_facility f ON c.Facility_ID = f.Facility_ID
JOIN dimensional.dim_measure m ON c.Measure_ID = m.Measure_ID
LEFT JOIN dimensional.dim_date d ON c.Start_Date = d.Date_Key;

SELECT * FROM dimensional.fact_hospital_quality LIMIT 10;

CREATE OR REPLACE TABLE dimensional.fact_hospital_payment (
    Payment_Fact_SK INT AUTOINCREMENT PRIMARY KEY,
    Facility_SK INT NOT NULL,
    Measure_SK INT NOT NULL,
    Date_SK INT NOT NULL,
    Payment FLOAT,
    Lower_Estimate FLOAT,
    Higher_Estimate FLOAT,
    Denominator INT,
    Payment_Category STRING,
    FOREIGN KEY (Facility_SK) REFERENCES dimensional.dim_facility(Facility_SK),
    FOREIGN KEY (Measure_SK) REFERENCES dimensional.dim_measure(Measure_SK),
    FOREIGN KEY (Date_SK) REFERENCES dimensional.dim_date(Date_SK)
);

INSERT INTO dimensional.fact_hospital_payment (
    Facility_SK, Measure_SK, Date_SK, Payment,
    Lower_Estimate, Higher_Estimate, Denominator, Payment_Category
)
SELECT
    f.Facility_SK,
    m.Measure_SK,
    COALESCE(d.Date_SK, -1), -- Assign a default -1 for unknown dates
    p.Score,
    p.Lower_Estimate,
    p.Higher_Estimate,
    p.Denominator,
    p.Payment_Category
FROM staging.stg_payments_facts p
JOIN dimensional.dim_facility f ON p.Facility_ID = f.Facility_ID
JOIN dimensional.dim_measure m ON p.Measure_ID = m.Measure_ID
LEFT JOIN dimensional.dim_date d ON p.Start_Date = d.Date_Key;

-- Query1
SELECT 
    o.Hospital_Ownership,
    COUNT(DISTINCT f.Facility_SK) AS Total_Hospitals,
    ROUND(AVG(q.Score), 2) AS Avg_Quality_Score,
    SUM(CASE WHEN q.Compared_to_National = 'Worse' THEN 1 ELSE 0 END) AS Worse_Than_National,
    SUM(CASE WHEN q.Compared_to_National = 'Better' THEN 1 ELSE 0 END) AS Better_Than_National,
    (SUM(CASE WHEN q.Compared_to_National = 'Better' THEN 1 ELSE 0 END) * 100.0) /
    NULLIF(COUNT(q.Quality_Fact_SK), 0) AS Percent_Better
FROM dimensional.fact_hospital_quality q
JOIN dimensional.dim_facility f ON q.Facility_SK = f.Facility_SK
JOIN dimensional.dim_hospital_ownership o ON f.Hospital_Ownership = o.Hospital_Ownership
JOIN dimensional.dim_measure m ON q.Measure_SK = m.Measure_SK
WHERE q.Fact_Type = 'Complication' -- Focus on complications
GROUP BY o.Hospital_Ownership
ORDER BY Percent_Better DESC;

--Query2
SELECT 
    o.Hospital_Ownership,
    t.Hospital_Type,
    COUNT(DISTINCT f.Facility_SK) AS Total_Hospitals,
    ROUND(AVG(q.Score), 2) AS Avg_Quality_Score,
    ROUND(AVG(p.Payment), 2) AS Avg_Payment,
    SUM(CASE WHEN q.Compared_to_National = 'Worse' THEN 1 ELSE 0 END) AS Worse_Than_National,
    SUM(CASE WHEN q.Compared_to_National = 'Better' THEN 1 ELSE 0 END) AS Better_Than_National,
    ROUND((SUM(CASE WHEN q.Compared_to_National = 'Better' THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(q.Quality_Fact_SK), 0), 2) AS Percent_Better,
    ROUND((SUM(CASE WHEN q.Compared_to_National = 'Worse' THEN 1 ELSE 0 END) * 100.0) / NULLIF(COUNT(q.Quality_Fact_SK), 0), 2) AS Percent_Worse
FROM dimensional.fact_hospital_quality q
JOIN dimensional.fact_hospital_payment p ON q.Facility_SK = p.Facility_SK
JOIN dimensional.dim_facility f ON q.Facility_SK = f.Facility_SK
JOIN dimensional.dim_hospital_ownership o ON f.Hospital_Ownership = o.Hospital_Ownership
JOIN dimensional.dim_hospital_type t ON f.Hospital_Type = t.Hospital_Type
WHERE q.Fact_Type = 'Complication' -- Focus on complications
GROUP BY o.Hospital_Ownership, t.Hospital_Type
ORDER BY Percent_Worse DESC, Avg_Quality_Score ASC;

--Query3
WITH InfectionRanks AS (
    SELECT 
        f.Facility_Name,
        f.State,
        t.Hospital_Type,
        i.Score AS Infection_Rate,
        q.Score AS Quality_Score,
        RANK() OVER (PARTITION BY f.State ORDER BY i.Score DESC) AS Infection_Rank,
        RANK() OVER (PARTITION BY f.State ORDER BY q.Score ASC) AS Quality_Rank
    FROM dimensional.fact_hospital_quality q
    JOIN dimensional.fact_hospital_quality i ON q.Facility_SK = i.Facility_SK
    JOIN dimensional.dim_facility f ON q.Facility_SK = f.Facility_SK
    JOIN dimensional.dim_hospital_type t ON f.Hospital_Type = t.Hospital_Type
    WHERE i.Score IS NOT NULL AND q.Score IS NOT NULL
),
WorstHospitals AS (
    SELECT 
        Facility_Name, State, Hospital_Type, Infection_Rate, Quality_Score
    FROM InfectionRanks
    WHERE Infection_Rank <= 10 AND Quality_Rank <= 10  -- Worst 10 hospitals per state
)
SELECT 
    State,
    Hospital_Type,
    COUNT(*) AS Num_Hospitals,
    ROUND(AVG(Infection_Rate), 2) AS Avg_Infection_Rate,
    ROUND(AVG(Quality_Score), 2) AS Avg_Quality_Score
FROM WorstHospitals
GROUP BY State, Hospital_Type
ORDER BY Avg_Infection_Rate DESC, Avg_Quality_Score ASC;

--Query 4
SELECT 
    f.State,
    t.Hospital_Type,
    COUNT(f.Facility_SK) AS Hospital_Count
FROM dimensional.dim_facility f
JOIN dimensional.dim_hospital_type t ON f.Hospital_Type = t.Hospital_Type
GROUP BY f.State, t.Hospital_Type
ORDER BY f.State, Hospital_Count DESC;