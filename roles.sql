create role dbt_role;
grant usage on warehouse compute_wh to role dbt_role;
show grants on warehouse compute_wh;
grant role dbt_role to user cesaramehri;
grant all on database RAW to role dbt_role;
grant all on database ANALYTICS_DW to role dbt_role;