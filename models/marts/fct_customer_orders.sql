with 

-- Import CTEs

base_customers as (

    select * from {{ source('jaffle_shop', 'customers') }}

),

base_orders as (

    select * from {{ source('jaffle_shop', 'orders') }}

),

base_payments as (

    select * from {{ source('stripe', 'payments') }}

),

completed_payments as (

    select 
        orderid as order_id, 
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid
    from base_payments
    where status <> 'fail'
    group by 1

),

paid_orders as (
    select orders.id as order_id,
        orders.user_id    as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.first_name    as customer_first_name,
        c.last_name as customer_last_name
    from base_orders as orders
        left join completed_payments p on orders.id = p.order_id
        left join base_customers c on orders.user_id = c.id
),

-- customer_orders 
--     as (
--         select 
--             base_customers.id as customer_id,
--             min(base_orders.order_date) as first_order_date,
--             max(base_orders.order_date) as most_recent_order_date,
--             count(base_orders.id) as number_of_orders
--         from base_customers
--             left join base_orders on base_orders.user_id = base_customers.id 
--         group by 1
-- ),

clv as (

    select
        p.order_id,
        sum(p.total_amount_paid) over (partition by p.customer_id order by order_id) as clv_bad
    from paid_orders p
    order by p.order_id

),

final as (

    select
        paid_orders.*,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
        case 
            when rank() over(partition by customer_id order by order_placed_at, paid_orders.order_id) = 1 then 'new'
            else 'return' 
            end as nvsr,
        clv.clv_bad as customer_lifetime_value,
        first_value(paid_orders.order_placed_at) 
            over(partition by paid_orders.customer_id order by paid_orders.order_placed_at) as fdos
    from paid_orders 
        -- left join customer_orders as c using (customer_id)
        left join clv on clv.order_id = paid_orders.order_id
    order by paid_orders.order_id

)

select * from final