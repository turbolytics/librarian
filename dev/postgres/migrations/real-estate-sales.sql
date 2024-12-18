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