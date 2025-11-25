Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- `dbt run`
- `dbt test`

### Tests Added

This project now includes comprehensive dbt tests:

#### Schema Tests (YAML-based)
1. **Staging models** (`models/stg/schema.yml`):
   - `stg_customers`: unique/not_null on `customer_id`, not_null on names
   - `stg_orders`: unique/not_null on `order_id`, not_null on keys/dates/status, accepted_values on `status`
   - `stg_payments`: unique/not_null on `payment_id`, not_null on keys/method/amount, accepted_values on `payment_method`

2. **Final models** (`models/schema.yml`):
   - `customers`: unique/not_null PK, relationships to `stg_customers`, not_null on metrics
   - `orders`: unique/not_null PK, relationships to `stg_customers`, accepted_values on `status`, not_null on amounts

#### Custom Singular Test
- `tests/order_payment_totals.sql`: Ensures payment method amounts sum to total `amount` in `orders`

### Running Tests

```bash
# Run all models then all tests
dbt run && dbt test

# Run specific tests
dbt test --select stg_customers
dbt test --select +orders  # Upstream and downstream of orders
dbt test --select test_type:singular  # Only singular tests

# Select by tags (add tags to schema.yml if needed)
dbt test --select tag:stg
```

### Data Flow
```
raw_customers/orders/payments (sources)
    ↓
stg_customers/orders/payments (staging + tests)
    ↓
customers (aggregated metrics + tests)
orders (payment breakdowns + tests)
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- [dbt Testing Guide](https://docs.getdbt.com/docs/build/tests)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
