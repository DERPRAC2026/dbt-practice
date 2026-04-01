{{ config(
    materialized='table',
    schema='MARTS'
) }}

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ source('raw', 'raw_customers') }}

),

final as (

    select
        o.order_id,
        o.ordered_at,
        o.order_amount,
        o.status,
        c.customer_id,
        c.full_name      as customer_name,
        c.email          as customer_email

    from orders o
    left join customers c
        on o.customer_id = c.customer_id

)

select * from final

/*```

Save it.

Notice two things here — `{{ config(materialized='table') }}` at the top tells dbt to create this as a physical **table** not a view. And `{{ ref('stg_orders') }}` tells dbt this model depends on `stg_orders` — so run that first.

---

## Step 5 — Run it

Your file tree should now look like:
```
models/
  staging/
    sources.yml
    stg_orders.sql
  marts/
    fct_orders.sql
  example/
    (ignore these)
```

In the command bar at the bottom type:
```
dbt run
```

Hit Enter. You'll see logs streaming. It should say something like:
```
1 of 2 START sql view model staging.stg_orders .............. OK
2 of 2 START sql table model marts.fct_orders ............... OK
2 of 2 OK created sql table model marts.fct_orders .......... SUCCESS */