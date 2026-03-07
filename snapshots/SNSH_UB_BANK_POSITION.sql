{%snapshot SNSH_UB_BANK_POSITION %}
{{
    config(
        unique_key = 'POSITION_HKEY',
        strategy = 'check',
        check_cols= ['POSITION_HDIFF'],
        invalidate_hard_deletions = True,
    )
}}
SELECT * FROM {{ref('v2_STG_UB_BANK_POSITION')}}
{% endsnapshot %}