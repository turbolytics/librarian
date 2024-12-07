CREATE TABLE property_sales
(
    serial_number    INTEGER,
    list_year        INTEGER,
    date_recorded    DATE,
    town             VARCHAR(50),
    address          VARCHAR(255),
    assessed_value   DECIMAL(12, 2),
    sale_amount      DECIMAL(12, 2),
    sales_ratio      DECIMAL(10, 2),
    property_type    VARCHAR(50),
    residential_type VARCHAR(50),
    non_use_code     VARCHAR(50),
    assessor_remarks VARCHAR(255),
    opm_remarks      VARCHAR(255),
    location         VARCHAR(255)
);

COPY property_sales(
    serial_number,
    list_year,
    date_recorded,
    town,
    address,
    assessed_value,
    sale_amount,
    sales_ratio,
    property_type,
    residential_type,
    non_use_code,
    assessor_remarks,
    opm_remarks,
    location
)
FROM '/tmp/data/Real_Estate_Sales_2001-2022_GL.csv'
DELIMITER ','
CSV HEADER;