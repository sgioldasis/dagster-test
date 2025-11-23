select *
from {{ ref('orders') }}
where 
    credit_card_amount + coupon_amount + bank_transfer_amount + gift_card_amount != amount