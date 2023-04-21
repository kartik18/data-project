DROP TABLE IF  EXISTS tmp_tbl ;

CREATE TABLE IF NOT EXISTS tmp_tbl AS
Select 
    client_name,
    delivery_address,
    delivery_city,
    delivery_postcode,
    delivery_country,
    delivery_contact_number,
    start_date,
    lead(start_date,1,'2262-04-11' ) over (PARTITION by client_name order by start_date) as end_date,
    CASE WHEN lead(start_date,1,'2262-04-11') over (PARTITION by client_name order by start_date) = '2262-04-11' THEN 1 ELSE 0 END AS is_current
from (
select
    client_name,
    delivery_address,
    delivery_city,
    delivery_postcode,
    delivery_country,
    delivery_contact_number,
    min(start_date) as start_date,
    max(start_date) as end_date
from stg_client_dim
group by 
    client_name,
    delivery_address,
    delivery_city,
    delivery_postcode,
    delivery_country,
    delivery_contact_number
) SUB;

UPDATE client_dim 
SET end_date = (
    SELECT  COALESCE(min(start_date), client_dim.end_date) FROM
    tmp_tbl
    WHERE tmp_tbl.client_name = client_dim.client_name
    AND tmp_tbl.start_date > client_dim.start_date
    AND client_dim.is_current=1
);

UPDATE client_dim
SET is_current = 0
WHERE EXISTS (
  SELECT 1 FROM tmp_tbl
  WHERE tmp_tbl.client_name = client_dim.client_name
  AND tmp_tbl.start_date > (
    SELECT MAX(start_date) FROM client_dim AS t2
    WHERE t2.client_name = client_dim.client_name
  )
);


INSERT INTO client_dim
select 
    tmp_tbl.client_name,
    tmp_tbl.delivery_address,
    tmp_tbl.delivery_city,
    tmp_tbl.delivery_postcode,
    tmp_tbl.delivery_country,
    tmp_tbl.delivery_contact_number,
    tmp_tbl.start_date,
    tmp_tbl.end_date,
    tmp_tbl.is_current 
from       
    tmp_tbl
LEFT JOIN
    (SELECT client_name, max(start_date) as max_date
    from client_dim
    group by client_name
    ) target_tbl
    on tmp_tbl.client_name = target_tbl.client_name
where target_tbl.max_date is null or tmp_tbl.start_date > target_tbl.max_date
;

