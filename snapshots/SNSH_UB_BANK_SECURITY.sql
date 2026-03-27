{%snapshot SNSH_UB_BANK_SECURITY %}
{{
    config(
        unique_key = 'POSITION_HKEY',
        strategy = 'check',
        check_cols= ['SECURITY_HDIFF'],
        
    )
}}
SELECT * FROM {{ref('STG_UB_BANK_SECURITY')}}
{% endsnapshot %}