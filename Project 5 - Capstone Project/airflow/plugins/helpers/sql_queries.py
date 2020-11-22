class SqlQueries:
    
           create_table_d_country= ("""
            CREATE TABLE IF NOT EXISTS public.d_country (
                country_id int4 NOT NULL,
                country_name varchar(200),
                CONSTRAINT country_pkey PRIMARY KEY (country_id)
                );
        """)

           create_table_d_state= ("""
            CREATE TABLE IF NOT EXISTS public.d_state (
                state_id varchar(10),
                state_name varchar(200),
                CONSTRAINT state_pkey PRIMARY KEY (state_id)
                );
        """)

           create_table_d_port= ("""
            CREATE TABLE IF NOT EXISTS public.d_port (
                port_id varchar(10),
                port_name varchar(200),
                CONSTRAINT port_pkey PRIMARY KEY (port_id)
                );
        """)
           create_table_final_immigration = ("""
            CREATE TABLE IF NOT EXISTS public.final_immigration (      
                        cicid int8,
                        i94yr int4,
                        i94cit int4,
                        i94res int4,
                        country_name varchar(100),
                        i94port varchar(4),
                        i94mode_description varchar(50),
                        state_name varchar(100),
                        i94addr varchar(128),
                        i94bir int4,
                        i94visa int4,
                        VISA_CATEGORY varchar(20),
                        visapost varchar(4),
                        entdepa varchar(4),
                        biryear int4,
                        gender varchar(100),
                        airline varchar(128),
                        visatype varchar(4),
                        arrival_date date,
                        departure_date date,
                        no_of_days_stayed int4,
                        CONSTRAINT final_immigration_airport_pkey PRIMARY KEY (cicid)
          );
        """)


           create_table_staging_immigration = ("""
             CREATE TABLE IF NOT EXISTS public.staging_immigration (
                cicid int8,
                i94yr int4,
                i94cit int4,
                i94res int4,
                i94port varchar(4),
                i94mode int4,
                i94addr varchar(128),
                i94bir int4,
                i94visa int4,
                visapost varchar(4),
                entdepa varchar(4),
                biryear int4,
                gender varchar(100),
                airline varchar(128),
                visatype varchar(4),
                arrival_date date,
                departure_date date,
                no_of_days_stayed int4
                );
              """)

           create_table_staging_airport = ("""
            CREATE TABLE IF NOT EXISTS public.staging_airport (
                    ident varchar(10),
                    type varchar(100),
                    name varchar(100),
                    elevation_ft int4,
                    continent varchar(4),
                    iso_country varchar(6),
                    iso_region  varchar(6),
                    municipality varchar(100),
                    gps_code varchar(10),
                    iata_code varchar(10),
                    local_code varchar(10),
                    coordinates  varchar(256)
                    );
                   """)
            
     

           create_table_staging_us_cities_demographics = ("""CREATE TABLE IF NOT EXISTS public.staging_us_cities_demographics(
                    City varchar(100),
                    State varchar(100),
                    Median_Age decimal(4,2),
                    Male_Population int4,
                    Female_Population int4,
                    Total_Population int4,
                    Number_of_Veterans int4,
                    Foreign_born int4,
                    Average_Household_Size decimal(4,2),
                    State_Code varchar(2),
                    Race varchar(50),
                    Count int4
                     );
                   """)

           final_immigration_table_insert = ("""
                SELECT
                    immigr.cicid AS CCCID,
                    immigr.i94yr AS YEAR,
                    immigr.i94cit AS CITY,
                    immigr.i94res AS COUNTRY_ID,
                    immi_country.country_name AS COUNTRY_NAME,
                    immigr.i94port AS I94PORT,
                    CASE
                        WHEN i94mode = 1 THEN 'Air'
                        WHEN i94mode = 2 THEN 'Sea'
                        WHEN i94mode = 3 THEN 'Land'
                     ELSE
                      'Not reported'
                    END AS i94mode_description,
                    immi_state.state_name as STATE_NAME,
                    immigr.i94addr as I94_ADDRESS,
                    immigr.i94bir AS AGE,
                    immigr.i94visa AS I94_VISA,
                    CASE
                      WHEN I94visa = 1 THEN 'Business'
                      WHEN i94visa = 2 THEN 'Pleasure'
                      WHEN i94visa = 3 THEN 'Student'
                     END AS VISA_CATEGORY,
                    immigr.visapost AS VISA_POST,
                    immigr.entdepa AS ENTDEPA,
                    immigr.biryear AS BIRTH_YEAR,
                    immigr.gender AS GENDER,
                    immigr.airline AS AIRLINE,
                    immigr.visatype AS VISA_TYPE,
                    immigr.arrival_date AS ARRIVAL_DATE,
                    immigr.departure_date AS DEPARTURE_DATE,
                    immigr.no_of_days_stayed AS NO_OF_DAYS_STAYED
                    FROM staging_immigration immigr
                    INNER JOIN d_country immi_country
                    ON immigr.i94res=immi_country.country_id
                    INNER JOIN d_state immi_state
                    ON immigr.i94addr=immi_state.state_id
                    INNER JOIN d_port immi_port
                    ON immigr.i94port=immi_port.port_id
                  """)
            
           create_table_D_AIRPORT = ("""
                  CREATE TABLE IF NOT EXISTS public.D_AIRPORT (
                    STATE_ID varchar(10),
                    name varchar(100),
                    iata_code varchar(10),
                    local_code varchar(10),
                    coordinates varchar(256),
                    elevation_ft int4
                );
           """)

           D_AIRPORT_INSERT=("""
                  SELECT DISTINCT
                      SUBSTRING(iso_region, 4,2) AS STATE_ID,
                      name AS AIRPORT_NAME,
                      iata_code AS IATA_CODE,
                      local_code AS LOCAL_CODE,
                      coordinates,
                      ELEVATION_FT
                    FROM staging_airport
                      """)
            
           create_table_D_CITY_DEMO = ("""
                  CREATE TABLE IF NOT EXISTS public.D_CITY_DEMO (
                        City varchar(100),
                        State varchar(100),
                        Median_Age decimal(4,2),
                        Male_Population int4,
                        Female_Population int4,
                        Total_Population int4,
                        Number_of_Veterans int4,
                        Foreign_born int4,
                        Average_Household_Size decimal(4,2),
                        State_Code varchar(2),
                        Race varchar(50),
                        AVG_COUNT int4

                );
           """)

           D_CITY_DEMO_INSERT=("""
                  SELECT DISTINCT
                      city as CITY_NAME,
                      state as STATE_NAME,
                      Median_Age as MEDIAN_AGE,
                      Male_Population as MALE_POPULATION,
                      Female_Population as FEMALE_POPULATION,
                      Total_Population as TOTAL_POPULATION,
                      Number_of_Veterans as NUMBER_OF_VETERANS,
                      Foreign_born as FOREIGN_BORN,
                      Average_Household_Size as AVERAGE_HOUSEHOLD_SIZE,
                      State_Code as STATE_CODE,
                      Race AS RACE,
                      AVG(Count) AS AVG_COUNT
                    FROM
                      staging_us_cities_demographics
                    GROUP BY
                      1,
                      2,
                      3,
                      4,
                      5,
                      6,
                      7,
                      8,
                      9,
                      10,
                      11    
      """)

      
            
           create_table_D_TIME = ("""
                 CREATE TABLE IF NOT EXISTS public.D_TIME (
                         ARRIVAL_DATE date,
                         DAY int4,
                         MONTH int4,
                         YEAR int4,
                         QUARTER int4,
                         DAYOFWEEK int4,
                         WEEK int4
                        );
                   """)

           D_TIME_INSERT=("""  
                SELECT
                  ARRIVAL_DATE,
                  EXTRACT(DAY FROM ARRIVAL_DATE) AS DAY,
                  EXTRACT(MONTH FROM ARRIVAL_DATE) AS MONTH,
                  EXTRACT(YEAR FROM ARRIVAL_DATE) AS YEAR,
                  EXTRACT(QUARTER FROM ARRIVAL_DATE) AS QUARTER,
                  EXTRACT(DAYOFWEEK FROM ARRIVAL_DATE) AS DAYOFWEEK,
                  EXTRACT(WEEK FROM ARRIVAL_DATE) AS WEEKOFYEAR
                    FROM (
                      SELECT
                        DISTINCT ARRIVAL_DATE
                      FROM
                        final_immigration
                      UNION DISTINCT
                      SELECT
                        DISTINCT DEPARTURE_DATE
                      FROM
                        final_immigration)
                     WHERE ARRIVAL_DATE IS NOT NULL
         """)

           insert_table_d_country= ("""
               SELECT country_id,
                    country_name
                    FROM d_country
                    );
            """)

           insert_table_d_state= ("""
                CREATE 
                    state_id,
                    state_name
                    FROM d_state
                    );
            """)

           insert_table_d_port= ("""
                CREATE port_id,
                    port_name 
                    FROM d_port);
                    """)


