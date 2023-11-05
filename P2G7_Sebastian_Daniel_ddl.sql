COPY table_gc7 
FROM 'C:\Users\USER\anaconda3\envs\rmt-23\gc7\p2-ftds023-rmt-g7-Legacy453\P2G7_sebastian_daniel_data_raw.csv' 
DELIMITER ',' 
CSV HEADER;

select *
from table_gc7